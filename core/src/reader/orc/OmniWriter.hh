#ifndef NATIVE_READER_OMNIWRITER_HH
#define NATIVE_READER_OMNIWRITER_HH

#include "orc/Common.hh"
#include "orc/orc-config.hh"
#include "orc/Type.hh"
#include "orc/Vector.hh"
#include "vector/vector.h"
#include "OmniColumnWriter.hh"
#include "reader/common/JulianGregorianRebase.h"
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace omniruntime::writer {

    /**
     * Per-writer ORC serialization settings supplied by Spark/Gluten through JNI.
     *
     * Defaults deliberately select the legacy serial path, so existing native
     * callers using the three-argument factory retain their original behavior.
     */
    struct OmniWriterRuntimeOptions {
        // Enables top-level column serialization in parallel.
        bool parallelSerializeEnabled = false;
        // Maximum number of serialization workers; values below one are normalized to one.
        uint32_t parallelSerializeMaxThreads = 1;
    };

    class OmniWriter {
    public:
        virtual ~OmniWriter();

        /**
         * Add a row batch into current writer.
         * @param rowsToAdd the row batch data to write.
         */
        virtual void add(omniruntime::vec::BaseVector *rowsToAdd, uint64_t startPos, uint64_t endPos) = 0;

        /**
         * Close the writer and flush any pending data to the output stream.
         */
        virtual void close() = 0;

        /**
         * Add user metadata to the writer.
         */
        virtual void addUserMetadata(const std::string name, const std::string value) = 0;
    };

    std::unique_ptr <OmniWriter>
    createOmniWriter(const ::orc::Type &type, ::orc::OutputStream *stream, const ::orc::WriterOptions &options);

    // Runtime-configurable overload used by JNI; the legacy overload above delegates here with defaults.
    std::unique_ptr <OmniWriter>
    createOmniWriter(
            const ::orc::Type &type,
            ::orc::OutputStream *stream,
            const ::orc::WriterOptions &options,
            const OmniWriterRuntimeOptions &runtimeOptions);

    std::unique_ptr <OmniWriter>
    createOmniWriterWithTimestampRebase(const ::orc::Type &type, ::orc::OutputStream *stream,
                                        const ::orc::WriterOptions &options,
                                        std::unique_ptr<common::JulianGregorianRebase> timestampRebase);

    // Combines timestamp rebasing with per-writer parallel serialization settings.
    std::unique_ptr <OmniWriter>
    createOmniWriterWithTimestampRebase(
            const ::orc::Type &type,
            ::orc::OutputStream *stream,
            const ::orc::WriterOptions &options,
            std::unique_ptr<common::JulianGregorianRebase> timestampRebase,
            const OmniWriterRuntimeOptions &runtimeOptions);
}
#endif
