#include "OrcReader.h"
#include "reader/Reader.h"
#include "OmniColReader.hh"

namespace omniruntime::reader {

std::unique_ptr<OrcReader> Create(
    std::shared_ptr <FileContents> &contents, std::unique_ptr <ORCBufferInput> &&orcBufferInput,
    const ::orc::ReaderOptions &options)
{
    std::unique_ptr<::orc::InputStream> stream = orcBufferInput->GetInputStream();
    contents = std::make_shared<FileContents>();
    contents->pool = options.getMemoryPool();
    contents->errorStream = options.getErrorStream();
    std::string serializedFooter = options.getSerializedFileTail();
    uint64_t fileLength;
    uint64_t postscriptLength;
    if (serializedFooter.length() != 0) {
        // Parse the file tail from the serialized one.
        ::orc::proto::FileTail tail;
        if (!tail.ParseFromString(serializedFooter)) {
            throw omniruntime::exception::OmniException("EXPRESSION_NOT_SUPPORT",
                                                        "Failed to parse the file tail from string");
        }
        contents->postscript.reset(new ::orc::proto::PostScript(tail.postscript()));
        contents->footer.reset(new ::orc::proto::Footer(tail.footer()));
        fileLength = tail.filelength();
        postscriptLength = tail.postscriptlength();
    } else {
        // figure out the size of the file using the option or filesystem
        fileLength = std::min(options.getTailLocation(),
                              static_cast<uint64_t>(stream->getLength()));

        // read last bytes into buffer to get PostScript
        uint64_t readSize = std::min(fileLength, ::orc::DIRECTORY_SIZE_GUESS);
        static constexpr uint64_t MIN_ORC_POSTSCRIPT_SIZE = 4;
        if (readSize < MIN_ORC_POSTSCRIPT_SIZE) {
            throw omniruntime::exception::OmniException("EXPRESSION_NOT_SUPPORT", "File size too small");
        }
        auto buffer = std::make_unique<DataBuffer<char>>(*contents->pool, readSize);
        stream->read(buffer->data(), readSize, fileLength - readSize);

        postscriptLength = buffer->data()[readSize - 1] & 0xff;
        contents->postscript = REDUNDANT_MOVE(readPostscript(stream.get(),
                                                             buffer.get(), postscriptLength));
        uint64_t footerSize = contents->postscript->footerlength();
        uint64_t tailSize = 1 + postscriptLength + footerSize;
        if (tailSize >= fileLength) {
            std::string msg =
                "Invalid ORC tailSize=" + std::to_string(tailSize) + ", fileLength=" + std::to_string(fileLength);
            throw omniruntime::exception::OmniException("EXPRESSION_NOT_SUPPORT", msg);
        }
        uint64_t footerOffset;

        if (tailSize > readSize) {
            buffer->resize(footerSize);
            stream->read(buffer->data(), footerSize, fileLength - tailSize);
            footerOffset = 0;
        } else {
            footerOffset = readSize - tailSize;
        }

        contents->footer = REDUNDANT_MOVE(readFooter(stream.get(), buffer.get(),
                                                     footerOffset, *contents->postscript, *contents->pool));
    }
    contents->stream = std::move(stream);
    return std::make_unique<OrcReader>(contents, options, fileLength, postscriptLength);
}

std::unique_ptr <RowReader> OrcReader::CreateRowReader(
    std::shared_ptr <RowReaderOptions> options,
    std::unique_ptr<common::JulianGregorianRebase> &julianPtr,
    std::unique_ptr<common::PredicateCondition> &predicate)
{
    std::shared_ptr<OrcRowReaderOptions> orcOptions = std::dynamic_pointer_cast<OrcRowReaderOptions>(options);
    auto rowReader = std::make_unique<OrcRowReader>(
        contents_, orcOptions->GetOrcRowReaderOptions(), julianPtr, predicate);
    return std::move(rowReader);
}

void OrcRowReader::StartNextStripe()
{
    reader.reset(); // ColumnReaders use lots of memory; free old memory first
    rowIndexes.clear();
    bloomFilterIndex.clear();

    do {
        currentStripeInfo = footer->stripes(static_cast<int>(currentStripe));
        uint64_t fileLength = contents_->stream->getLength();
        if (currentStripeInfo.offset() + currentStripeInfo.indexlength() +
            currentStripeInfo.datalength() + currentStripeInfo.footerlength() >= fileLength) {
            std::stringstream msg;
            msg << "Malformed StripeInformation at stripe index " << currentStripe << ": fileLength="
                << fileLength << ", StripeInfo=(offset=" << currentStripeInfo.offset() << ", indexLength="
                << currentStripeInfo.indexlength() << ", dataLength=" << currentStripeInfo.datalength()
                << ", footerLength=" << currentStripeInfo.footerlength() << ")";
            throw ::orc::ParseError(msg.str());
        }
        currentStripeFooter = getStripeFooter(currentStripeInfo, *contents_.get());
        rowsInCurrentStripe = currentStripeInfo.numberofrows();

        if (sargsApplier) {
            // read row group statistics and bloom filters of current stripe
            loadStripeIndex();
            // select row groups to read in the current stripe
            sargsApplier->pickRowGroups(rowsInCurrentStripe, rowIndexes, bloomFilterIndex);
            if (sargsApplier->hasSelectedFrom(currentRowInStripe)) {
                // current stripe has at least one row group matching the predicate
                break;
            } else {
                // advance to next stripe when current stripe has no matching rows
                currentStripe += 1;
                currentRowInStripe = 0;
            }
        }
    } while (sargsApplier && currentStripe < lastStripe);

    if (currentStripe < lastStripe) {
        // get writer timezone info from stripe footer to help understand timestamp values.
        const ::orc::Timezone &writerTimezone =
            currentStripeFooter.has_writertimezone() ?
            ::orc::getTimezoneByName(currentStripeFooter.writertimezone()) :
            localTimezone;
        ::orc::StripeStreamsImpl stripeStreams(*this, currentStripe, currentStripeInfo,
                                               currentStripeFooter, currentStripeInfo.offset(),
                                               *contents_->stream, writerTimezone,
                                               readerTimezone);
        reader = omniruntime::reader::omniBuildReader(*contents_->schema, stripeStreams,
            (julianPtr == nullptr) ? nullptr : julianPtr.get());

        if (sargsApplier) {
            // move to the 1st selected row group when PPD is enabled.
            currentRowInStripe = advanceToNextRowGroup(currentRowInStripe, rowsInCurrentStripe,
                                                       footer->rowindexstride(), sargsApplier->getRowGroups());
            previousRow = firstRowOfStripe[currentStripe] + currentRowInStripe - 1;
            if (currentRowInStripe > 0) {
                seekToRowGroup(static_cast<uint32_t>(currentRowInStripe / footer->rowindexstride()));
            }
        }
    }
}

uint64_t OrcRowReader::Next(std::vector<omniruntime::vec::BaseVector *> *batch, int *omniTypeId, uint64_t batchLen)
{
    if (currentStripe >= lastStripe) {
        if (lastStripe > 0) {
            previousRow = firstRowOfStripe[lastStripe - 1] +
                          footer->stripes(static_cast<int>(lastStripe - 1)).numberofrows();
        } else {
            previousRow = 0;
        }
        return false;
    }
    if (currentRowInStripe == 0) {
        StartNextStripe();
    }

    uint64_t rowsToRead = std::min(batchLen, rowsInCurrentStripe - currentRowInStripe);
    if (sargsApplier) {
        rowsToRead = computeBatchSize(rowsToRead, currentRowInStripe, rowsInCurrentStripe,
                                      footer->rowindexstride(), sargsApplier->getRowGroups());
    }
    if (rowsToRead == 0) {
        previousRow = lastStripe <= 0 ? footer->numberofrows() :
                      firstRowOfStripe[lastStripe - 1] +
                      footer->stripes(static_cast<int>(lastStripe - 1)).numberofrows();
        return rowsToRead;
    }
    if (enableEncodedBlock) {
        throw omniruntime::exception::OmniException("EXPRESSION_NOT_SUPPORT", "enableEncodedBlock is not finished!!!");
    } else {
        const ::orc::Type &baseTp = this->getSelectedType();
        reader->next(reinterpret_cast<void *&>(batch), rowsToRead, nullptr, baseTp, omniTypeId);
    }
    previousRow = firstRowOfStripe[currentStripe] + currentRowInStripe;
    currentRowInStripe += rowsToRead;
    if (sargsApplier) {
        uint64_t nextRowToRead = advanceToNextRowGroup(currentRowInStripe, rowsInCurrentStripe,
                                                       footer->rowindexstride(), sargsApplier->getRowGroups());
        if (currentRowInStripe != nextRowToRead) {
            // it is guaranteed to be at start of a row group
            currentRowInStripe = nextRowToRead;
            if (currentRowInStripe < rowsInCurrentStripe) {
                seekToRowGroup(static_cast<uint32_t>(currentRowInStripe / footer->rowindexstride()));
            }
        }
    }
    if (currentRowInStripe >= rowsInCurrentStripe) {
        currentStripe += 1;
        currentRowInStripe = 0;
    }
    return rowsToRead;
}
}