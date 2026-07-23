/**
 * In-memory ORC OutputStream for per-column stripe buffers in parallel serialization.
 */
#ifndef OMNI_ORC_MEMORY_OUTPUT_STREAM_HH
#define OMNI_ORC_MEMORY_OUTPUT_STREAM_HH

#include "orc/OrcFile.hh"
#include <cstddef>
#include <stdexcept>
#include <string>
#include <vector>

namespace omniruntime::writer {

    /**
     * Growable output owned by one top-level column writer.
     *
     * A parallel flush never lets two child writers touch the same stream. After
     * all workers finish, the root struct copies these buffers to the real output
     * in schema order, preserving ORC stream layout deterministically.
     *
     * This class intentionally provides no internal synchronization: each instance
     * has a single child-writer owner during serialization, and merging starts only
     * after the worker barrier.
     */
    class OmniMemoryOutputStream : public ::orc::OutputStream {
    public:
        OmniMemoryOutputStream() = default;

        uint64_t getLength() const override {
            return static_cast<uint64_t>(buffer_.size());
        }

        uint64_t getNaturalWriteSize() const override {
            return 1024 * 1024;
        }

        void write(const void *buf, size_t length) override {
            if (length == 0) {
                return;
            }
            if (buf == nullptr) {
                throw std::invalid_argument("OmniMemoryOutputStream::write received null buffer.");
            }
            const auto *p = static_cast<const char *>(buf);
            buffer_.insert(buffer_.end(), p, p + length);
        }

        const std::string &getName() const override {
            static const std::string kName = "OmniMemoryOutputStream";
            return kName;
        }

        void close() override {}

        // Exposes contiguous bytes for the post-flush ordered merge.
        const char *data() const {
            return buffer_.data();
        }

        size_t size() const {
            return buffer_.size();
        }

        void clear() {
            // Capacity may be retained by std::vector for reuse by the next stripe.
            buffer_.clear();
        }

    private:
        std::vector<char> buffer_;
    };

} // namespace omniruntime::writer

#endif
