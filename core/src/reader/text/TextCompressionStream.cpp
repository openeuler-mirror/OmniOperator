/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 */
#include "reader/text/TextCompressionStream.h"

#include <arrow/result.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

#include <lz4.h>
#include <zlib.h>

#include "io/wrap/snappy_wrapper.h"
#include "util/omni_exception.h"

namespace omniruntime::reader::text {

using omniruntime::exception::OmniException;

namespace {

constexpr size_t STREAM_BUFFER_SIZE = 64 * 1024;
constexpr size_t INITIAL_LZ4_DECODE_BUFFER_SIZE = 1024 * 1024;

void CheckStatus(const arrow::Status& status)
{
    if (!status.ok()) {
        throw OmniException(status.ToString().c_str());
    }
}

class CompressedFileInput {
public:
    explicit CompressedFileInput(std::shared_ptr<arrow::io::RandomAccessFile> file)
        : file_(std::move(file))
    {
        if (file_ == nullptr) {
            throw std::runtime_error("Compressed Text input file is null.");
        }
    }

    int64_t Read(uint8_t* output, int64_t size)
    {
        if (size <= 0) {
            return 0;
        }
        auto result = file_->ReadAt(offset_, size, output);
        if (!result.ok()) {
            throw OmniException(result.status().ToString().c_str());
        }
        const auto bytes = std::move(result).ValueUnsafe();
        offset_ += bytes;
        return bytes;
    }

    bool ReadExact(uint8_t* output, int64_t size, bool allowEof)
    {
        int64_t received = 0;
        while (received < size) {
            const auto bytes = Read(output + received, size - received);
            if (bytes == 0) {
                if (allowEof && received == 0) {
                    return false;
                }
                throw std::runtime_error("Truncated compressed Text input.");
            }
            received += bytes;
        }
        return true;
    }

    int64_t RemainingBytes()
    {
        if (fileSize_ < 0) {
            auto result = file_->GetSize();
            if (!result.ok()) {
                throw OmniException(result.status().ToString().c_str());
            }
            fileSize_ = std::move(result).ValueUnsafe();
        }
        return std::max<int64_t>(0, fileSize_ - offset_);
    }

private:
    std::shared_ptr<arrow::io::RandomAccessFile> file_;
    int64_t offset_ = 0;
    int64_t fileSize_ = -1;
};

uint32_t DecodeBigEndian(const uint8_t* bytes)
{
    return (static_cast<uint32_t>(bytes[0]) << 24U) |
        (static_cast<uint32_t>(bytes[1]) << 16U) |
        (static_cast<uint32_t>(bytes[2]) << 8U) |
        static_cast<uint32_t>(bytes[3]);
}

std::array<uint8_t, 4> EncodeBigEndian(uint32_t value)
{
    return {static_cast<uint8_t>(value >> 24U), static_cast<uint8_t>(value >> 16U),
        static_cast<uint8_t>(value >> 8U), static_cast<uint8_t>(value)};
}

class ZlibInputStream final : public TextSequentialInput {
public:
    ZlibInputStream(std::shared_ptr<arrow::io::RandomAccessFile> file, bool gzip)
        : source_(std::move(file)), gzip_(gzip), input_(STREAM_BUFFER_SIZE)
    {
        std::memset(&stream_, 0, sizeof(stream_));
        const auto result = inflateInit2(&stream_, gzip_ ? MAX_WBITS + 16 : MAX_WBITS);
        if (result != Z_OK) {
            throw std::runtime_error("Failed to initialize Text zlib decompressor.");
        }
        initialized_ = true;
    }

    ~ZlibInputStream() override
    {
        if (initialized_) {
            inflateEnd(&stream_);
        }
    }

    int64_t Read(uint8_t* output, int64_t maxBytes) override
    {
        if (output == nullptr || maxBytes <= 0) {
            throw std::runtime_error("Invalid Text decompression output buffer.");
        }
        if (eof_) {
            return 0;
        }
        const auto capacity = static_cast<uInt>(std::min<int64_t>(
            maxBytes, std::numeric_limits<uInt>::max()));
        stream_.next_out = output;
        stream_.avail_out = capacity;
        while (stream_.avail_out == capacity && !eof_) {
            if (stream_.avail_in == 0 && !inputEof_) {
                const auto bytes = source_.Read(input_.data(), input_.size());
                inputEof_ = bytes == 0;
                stream_.next_in = input_.data();
                stream_.avail_in = static_cast<uInt>(bytes);
            }
            if (stream_.avail_in == 0 && inputEof_) {
                throw std::runtime_error("Truncated Text zlib stream.");
            }
            const auto beforeIn = stream_.avail_in;
            const auto result = inflate(&stream_, Z_NO_FLUSH);
            if (result == Z_STREAM_END) {
                if (!gzip_) {
                    eof_ = true;
                    break;
                }
                if (stream_.avail_in == 0 && !inputEof_) {
                    const auto bytes = source_.Read(input_.data(), input_.size());
                    inputEof_ = bytes == 0;
                    stream_.next_in = input_.data();
                    stream_.avail_in = static_cast<uInt>(bytes);
                }
                if (stream_.avail_in == 0 && inputEof_) {
                    eof_ = true;
                    break;
                }
                auto* remaining = stream_.next_in;
                const auto remainingSize = stream_.avail_in;
                if (inflateReset2(&stream_, MAX_WBITS + 16) != Z_OK) {
                    throw std::runtime_error("Failed to reset concatenated Gzip stream.");
                }
                stream_.next_in = remaining;
                stream_.avail_in = remainingSize;
                continue;
            }
            if (result != Z_OK && result != Z_BUF_ERROR) {
                throw std::runtime_error(std::string("Invalid Text zlib stream: ") +
                    (stream_.msg == nullptr ? "unknown error" : stream_.msg));
            }
            if (beforeIn == stream_.avail_in && stream_.avail_out == capacity) {
                throw std::runtime_error("Text zlib decompressor made no progress.");
            }
        }
        return static_cast<int64_t>(capacity - stream_.avail_out);
    }

private:
    CompressedFileInput source_;
    bool gzip_ = false;
    bool initialized_ = false;
    bool inputEof_ = false;
    bool eof_ = false;
    z_stream stream_{};
    std::vector<uint8_t> input_;
};

class HadoopBlockInputStream final : public TextSequentialInput {
public:
    HadoopBlockInputStream(
        std::shared_ptr<arrow::io::RandomAccessFile> file, TextCompressionKind codec)
        : source_(std::move(file)), codec_(codec)
    {
    }

    int64_t Read(uint8_t* output, int64_t maxBytes) override
    {
        if (output == nullptr || maxBytes <= 0) {
            throw std::runtime_error("Invalid Hadoop block decompression output buffer.");
        }
        int64_t written = 0;
        while (written < maxBytes) {
            if (decodedOffset_ < decoded_.size()) {
                const auto bytes = std::min<int64_t>(
                    maxBytes - written, decoded_.size() - decodedOffset_);
                std::memcpy(output + written, decoded_.data() + decodedOffset_, bytes);
                decodedOffset_ += static_cast<size_t>(bytes);
                written += bytes;
                continue;
            }
            if (eof_ || !LoadChunk()) {
                break;
            }
        }
        return written;
    }

private:
    bool LoadChunk()
    {
        decoded_.clear();
        decodedOffset_ = 0;
        if (blockRemaining_ == 0) {
            std::array<uint8_t, 4> header{};
            if (!source_.ReadExact(header.data(), header.size(), true)) {
                eof_ = true;
                return false;
            }
            blockRemaining_ = DecodeBigEndian(header.data());
            if (blockRemaining_ == 0) {
                eof_ = true;
                return false;
            }
        }

        std::array<uint8_t, 4> header{};
        source_.ReadExact(header.data(), header.size(), false);
        const auto compressedSize = DecodeBigEndian(header.data());
        if (compressedSize == 0 || compressedSize > static_cast<uint32_t>(std::numeric_limits<int>::max())) {
            throw std::runtime_error("Invalid Hadoop Text compressed chunk length.");
        }
        // Reject truncated chunks before allocating from an untrusted length header.
        if (static_cast<int64_t>(compressedSize) > source_.RemainingBytes()) {
            throw std::runtime_error("Truncated Hadoop Text compressed chunk.");
        }
        compressed_.resize(compressedSize);
        source_.ReadExact(compressed_.data(), compressed_.size(), false);

        size_t decodedSize = 0;
        if (codec_ == TextCompressionKind::SNAPPY) {
            if (!snappy::GetUncompressedLength(
                    reinterpret_cast<const char*>(compressed_.data()), compressed_.size(), &decodedSize) ||
                decodedSize == 0 || decodedSize > blockRemaining_) {
                throw std::runtime_error("Invalid Hadoop Snappy Text block.");
            }
            decoded_.resize(decodedSize);
            if (!snappy::RawUncompress(reinterpret_cast<const char*>(compressed_.data()),
                    compressed_.size(), reinterpret_cast<char*>(decoded_.data()))) {
                throw std::runtime_error("Failed to decompress Hadoop Snappy Text block.");
            }
        } else {
            if (blockRemaining_ > static_cast<uint32_t>(std::numeric_limits<int>::max())) {
                throw std::runtime_error("Hadoop LZ4 Text block is too large.");
            }
            auto capacity = std::min<size_t>(
                blockRemaining_, INITIAL_LZ4_DECODE_BUFFER_SIZE);
            int result = 0;
            while (capacity <= blockRemaining_) {
                decoded_.resize(capacity);
                result = LZ4_decompress_safe(
                    reinterpret_cast<const char*>(compressed_.data()),
                    reinterpret_cast<char*>(decoded_.data()),
                    static_cast<int>(compressed_.size()), static_cast<int>(decoded_.size()));
                if (result > 0 || capacity == blockRemaining_) {
                    break;
                }
                capacity = std::min<size_t>(blockRemaining_, capacity * 2);
            }
            if (result <= 0) {
                throw std::runtime_error("Failed to decompress Hadoop LZ4 Text block.");
            }
            decodedSize = static_cast<size_t>(result);
            decoded_.resize(decodedSize);
        }
        blockRemaining_ -= static_cast<uint32_t>(decodedSize);
        return true;
    }

    CompressedFileInput source_;
    TextCompressionKind codec_;
    bool eof_ = false;
    uint32_t blockRemaining_ = 0;
    std::vector<uint8_t> compressed_;
    std::vector<uint8_t> decoded_;
    size_t decodedOffset_ = 0;
};

class PlainTextOutputSink final : public TextOutputSink {
public:
    explicit PlainTextOutputSink(std::shared_ptr<arrow::io::OutputStream> output)
        : output_(std::move(output))
    {
    }

    void Write(const uint8_t* data, int64_t size) override
    {
        CheckStatus(output_->Write(data, size));
    }

    void Finish() override {}

    void Close() override
    {
        if (!closed_) {
            CheckStatus(output_->Close());
            closed_ = true;
        }
    }

private:
    std::shared_ptr<arrow::io::OutputStream> output_;
    bool closed_ = false;
};

class ZlibOutputSink final : public TextOutputSink {
public:
    ZlibOutputSink(std::shared_ptr<arrow::io::OutputStream> output, bool gzip)
        : output_(std::move(output)), outputBuffer_(STREAM_BUFFER_SIZE)
    {
        std::memset(&stream_, 0, sizeof(stream_));
        const auto result = deflateInit2(&stream_, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
            gzip ? MAX_WBITS + 16 : MAX_WBITS, 8, Z_DEFAULT_STRATEGY);
        if (result != Z_OK) {
            throw std::runtime_error("Failed to initialize Text zlib compressor.");
        }
        initialized_ = true;
    }

    ~ZlibOutputSink() override
    {
        if (initialized_) {
            deflateEnd(&stream_);
        }
    }

    void Write(const uint8_t* data, int64_t size) override
    {
        if (finished_ || data == nullptr || size < 0) {
            throw std::runtime_error("Invalid write to Text zlib stream.");
        }
        while (size > 0) {
            const auto chunk = static_cast<uInt>(std::min<int64_t>(
                size, std::numeric_limits<uInt>::max()));
            stream_.next_in = const_cast<Bytef*>(data);
            stream_.avail_in = chunk;
            uInt remainingInput = stream_.avail_in;
            while (remainingInput > 0) {
                Deflate(Z_NO_FLUSH);
                if (stream_.avail_in >= remainingInput) {
                    throw std::runtime_error("Text zlib compressor made no input progress.");
                }
                remainingInput = stream_.avail_in;
            }
            data += chunk;
            size -= chunk;
        }
    }

    void Finish() override
    {
        if (finished_) {
            return;
        }
        int result = Z_OK;
        while (result != Z_STREAM_END) {
            result = Deflate(Z_FINISH);
        }
        finished_ = true;
    }

    void Close() override
    {
        if (closed_) {
            return;
        }
        Finish();
        CheckStatus(output_->Close());
        closed_ = true;
    }

private:
    int Deflate(int flush)
    {
        stream_.next_out = outputBuffer_.data();
        stream_.avail_out = static_cast<uInt>(outputBuffer_.size());
        const auto result = deflate(&stream_, flush);
        if (result != Z_OK && result != Z_STREAM_END) {
            throw std::runtime_error("Failed to compress Text zlib stream.");
        }
        const auto bytes = outputBuffer_.size() - stream_.avail_out;
        if (bytes > 0) {
            CheckStatus(output_->Write(outputBuffer_.data(), bytes));
        }
        return result;
    }

    std::shared_ptr<arrow::io::OutputStream> output_;
    z_stream stream_{};
    std::vector<uint8_t> outputBuffer_;
    bool initialized_ = false;
    bool finished_ = false;
    bool closed_ = false;
};

class HadoopBlockOutputSink final : public TextOutputSink {
public:
    HadoopBlockOutputSink(std::shared_ptr<arrow::io::OutputStream> output,
        TextCompressionKind codec, uint32_t bufferSize)
        : output_(std::move(output)), codec_(codec)
    {
        const auto overhead = codec_ == TextCompressionKind::SNAPPY
            ? bufferSize / 6U + 32U
            : bufferSize / 255U + 16U;
        if (bufferSize <= overhead) {
            throw std::runtime_error("Hadoop Text compression buffer is too small.");
        }
        maxInputSize_ = bufferSize - overhead;
        if (codec_ == TextCompressionKind::LZ4 && maxInputSize_ > LZ4_MAX_INPUT_SIZE) {
            throw std::runtime_error("Hadoop LZ4 Text compression buffer is too large.");
        }
        pending_.reserve(maxInputSize_);
    }

    void Write(const uint8_t* data, int64_t size) override
    {
        if (finished_ || data == nullptr || size < 0) {
            throw std::runtime_error("Invalid write to Hadoop Text block stream.");
        }
        while (size > 0) {
            const auto bytes = std::min<int64_t>(size, maxInputSize_ - pending_.size());
            pending_.insert(pending_.end(), data, data + bytes);
            data += bytes;
            size -= bytes;
            if (pending_.size() == maxInputSize_) {
                FlushBlock();
            }
        }
    }

    void Finish() override
    {
        if (finished_) {
            return;
        }
        if (!pending_.empty()) {
            FlushBlock();
        } else if (!wroteBlock_) {
            WriteInt(0);
        }
        finished_ = true;
    }

    void Close() override
    {
        if (closed_) {
            return;
        }
        Finish();
        CheckStatus(output_->Close());
        closed_ = true;
    }

private:
    void WriteInt(uint32_t value)
    {
        const auto bytes = EncodeBigEndian(value);
        CheckStatus(output_->Write(bytes.data(), bytes.size()));
    }

    void FlushBlock()
    {
        if (pending_.empty()) {
            return;
        }
        if (pending_.size() > static_cast<size_t>(std::numeric_limits<int>::max())) {
            throw std::runtime_error("Hadoop Text compression block is too large.");
        }
        WriteInt(static_cast<uint32_t>(pending_.size()));
        if (codec_ == TextCompressionKind::SNAPPY) {
            compressed_.resize(snappy::MaxCompressedLength(pending_.size()));
            size_t compressedSize = 0;
            snappy::RawCompress(reinterpret_cast<const char*>(pending_.data()), pending_.size(),
                reinterpret_cast<char*>(compressed_.data()), &compressedSize);
            compressed_.resize(compressedSize);
        } else {
            compressed_.resize(LZ4_compressBound(static_cast<int>(pending_.size())));
            const auto compressedSize = LZ4_compress_default(
                reinterpret_cast<const char*>(pending_.data()),
                reinterpret_cast<char*>(compressed_.data()), static_cast<int>(pending_.size()),
                static_cast<int>(compressed_.size()));
            if (compressedSize <= 0) {
                throw std::runtime_error("Failed to compress Hadoop LZ4 Text block.");
            }
            compressed_.resize(static_cast<size_t>(compressedSize));
        }
        WriteInt(static_cast<uint32_t>(compressed_.size()));
        CheckStatus(output_->Write(compressed_.data(), compressed_.size()));
        pending_.clear();
        wroteBlock_ = true;
    }

    std::shared_ptr<arrow::io::OutputStream> output_;
    TextCompressionKind codec_;
    size_t maxInputSize_ = 0;
    std::vector<uint8_t> pending_;
    std::vector<uint8_t> compressed_;
    bool wroteBlock_ = false;
    bool finished_ = false;
    bool closed_ = false;
};

} // namespace

std::unique_ptr<TextSequentialInput> CreateTextSequentialInput(
    std::shared_ptr<arrow::io::RandomAccessFile> file, TextCompressionKind codec)
{
    switch (codec) {
        case TextCompressionKind::GZIP:
            return std::make_unique<ZlibInputStream>(std::move(file), true);
        case TextCompressionKind::DEFLATE:
            return std::make_unique<ZlibInputStream>(std::move(file), false);
        case TextCompressionKind::SNAPPY:
        case TextCompressionKind::LZ4:
            return std::make_unique<HadoopBlockInputStream>(std::move(file), codec);
        default:
            throw std::runtime_error("Unsupported compressed Text input codec.");
    }
}

std::unique_ptr<TextOutputSink> CreateTextOutputSink(
    std::shared_ptr<arrow::io::OutputStream> output,
    TextCompressionKind codec,
    uint32_t blockSize)
{
    switch (codec) {
        case TextCompressionKind::NONE:
            return std::make_unique<PlainTextOutputSink>(std::move(output));
        case TextCompressionKind::GZIP:
            return std::make_unique<ZlibOutputSink>(std::move(output), true);
        case TextCompressionKind::DEFLATE:
            return std::make_unique<ZlibOutputSink>(std::move(output), false);
        case TextCompressionKind::SNAPPY:
        case TextCompressionKind::LZ4:
            return std::make_unique<HadoopBlockOutputSink>(std::move(output), codec, blockSize);
        default:
            throw std::runtime_error("Unsupported compressed Text output codec.");
    }
}

} // namespace omniruntime::reader::text
