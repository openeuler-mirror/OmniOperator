#include "OrcReaderFactory.h"

namespace omniruntime::reader {

std::unique_ptr<OrcReader> Create(
    std::shared_ptr <FileContents> &contents, std::unique_ptr <ORCBufferInput> &&orcBufferInput,
    const std::shared_ptr<ReaderOptions> &options)
{
    auto orcReaderOptions = options->GetOrcReaderOptions();
    std::unique_ptr<::orc::InputStream> stream = orcBufferInput->GetInputStream();
    contents = std::make_shared<FileContents>();
    contents->pool = orcReaderOptions.getMemoryPool();
    contents->errorStream = orcReaderOptions.getErrorStream();
    std::string serializedFooter = orcReaderOptions.getSerializedFileTail();
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
        fileLength = std::min(orcReaderOptions.getTailLocation(),
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

}  // namespace omniruntime::reader
