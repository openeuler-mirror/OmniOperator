#ifndef OMNIOPERATORJIT_BUFFERINPUT_H
#define OMNIOPERATORJIT_BUFFERINPUT_H

#include <orc/io/InputStream.hh>
namespace omniruntime::reader {

class BufferInput {
public:
    virtual ~BufferInput() = default;

    virtual uint64_t GetSize() const = 0;
};

class ORCBufferInput : public BufferInput {
public:

    ORCBufferInput(const char *data, uint64_t size)
        : data_(data), size_(size), position_(0) {}

    explicit ORCBufferInput(std::unique_ptr<::orc::InputStream> inputStream)
        : inputStream_(std::move(inputStream)) {}

    std::unique_ptr<::orc::InputStream> GetInputStream()
    {
        return std::move(inputStream_);
    }

    uint64_t GetLength() const
    {
        return size_;
    }

    uint64_t GetSize() const override
    {
        return size_;
    }

private:
    std::unique_ptr<::orc::InputStream> inputStream_;
    const char *data_;
    uint64_t size_;
    uint64_t position_;
};
}
#endif // OMNIOPERATORJIT_BUFFERINPUT_H
