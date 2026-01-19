/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: codegen test
 */

#include <gtest/gtest.h>
#include "io/ColumnWriter.hh"
#include "io/Decompression.hh"
using namespace omniSpark;
uint64_t get_file_size(const std::string& file_path) {
    struct stat file_stat;
    if (stat(file_path.c_str(), &file_stat) != 0) {
        return 0;
    }
    return static_cast<uint64_t>(file_stat.st_size);
}

std::pair<size_t, bool> parseCompressionHeader(const char* header_buf) {
    size_t compressedSize = 0;
    bool is_original = false;

    compressedSize |= (static_cast<uint8_t>(header_buf[0]) >> 1);
    is_original = (static_cast<uint8_t>(header_buf[0]) & 0x01) == 1;

    compressedSize |= (static_cast<uint8_t>(header_buf[1]) << 7);

    compressedSize |= (static_cast<uint8_t>(header_buf[2]) << 15);

    return {compressedSize, is_original};
}


TEST(CompressionTest, LZ4_Compress_Decompress_Success) {
    const std::string original_data =
            "Hello, LZ4 Compression Test! This is a long test string to verify LZ4 compress and decompress function. ";
    const std::string file_path = "test_lz4_compress.data";
    const int32_t compress_block_size = 128 * 1024;

    std::cout << "original_data size: " << original_data.size() << " bytes" << std::endl;

    std::unique_ptr<OutputStream> out_stream = writeLocalFile(file_path);
    ASSERT_NE(out_stream, nullptr) << "Failed to create output stream";

    WriterOptions writer_options;
    writer_options.setCompression(CompressionKind_LZ4);
    writer_options.setCompressionBlockSize(compress_block_size);
    writer_options.setCompressionStrategy(CompressionStrategy_COMPRESSION);

    std::unique_ptr<StreamsFactory> stream_factory = createStreamsFactory(writer_options, out_stream.get());
    ASSERT_NE(stream_factory, nullptr) << "Failed to create streams factory";

    std::unique_ptr<BufferedOutputStream> buffered_stream = stream_factory->createStream();
    ASSERT_NE(buffered_stream, nullptr) << "Failed to create buffered stream";

    size_t remaining_len = original_data.size();
    const char* data_ptr = original_data.data();
    void* buffer = nullptr;
    int buffer_size = 0;

    while (remaining_len > 0 && buffered_stream->Next(&buffer, &buffer_size)) {
        size_t write_len = std::min(remaining_len, static_cast<size_t>(buffer_size));
        memcpy(buffer, data_ptr, write_len);
        remaining_len -= write_len;
        data_ptr += write_len;

        if (remaining_len == 0 && write_len < static_cast<size_t>(buffer_size)) {
            buffered_stream->BackUp(static_cast<int>(buffer_size - write_len));
        }
    }
    ASSERT_EQ(remaining_len, 0) << "Not all data was written to compressed stream";

    buffered_stream->flush();
    buffered_stream.reset();
    stream_factory.reset();
    uint64_t flush_total_size = get_file_size(file_path);
    out_stream->close();

    std::cout << "Total compressed file size: " << flush_total_size << " bytes" << std::endl;
    ASSERT_GT(flush_total_size, 3) << "Compressed file size too small (less than 3-byte header)";

    std::unique_ptr<InputStream> in_stream = readLocalFile(file_path);
    ASSERT_NE(in_stream, nullptr) << "Failed to open compressed file";

    char* compressed_data = new char[flush_total_size];
    in_stream->read(compressed_data, flush_total_size, 0);

    std::pair<size_t, bool> header_info = parseCompressionHeader(compressed_data);
    size_t compressed_block_size = header_info.first;
    bool is_original_data = header_info.second;
    std::cout << "Parsed header: compressed_block_size=" << compressed_block_size
              << ", is_original_data=" << (is_original_data ? "true" : "false") << std::endl;

    char* decompressed_data = nullptr;
    int32_t actual_decompress_len = 0;
    int32_t decompress_buf_size = static_cast<int32_t>(original_data.size() * 2);

    if (is_original_data) {
        decompressed_data = new char[compressed_block_size];
        memcpy(decompressed_data, compressed_data + 3, compressed_block_size); // 跳过3字节头部
        actual_decompress_len = static_cast<int32_t>(compressed_block_size);
        std::cout << "Compression no gain, use original data directly" << std::endl;
    } else {
        const char* lz4_raw_data = compressed_data + 3;
        auto [lz4_decomp_data, lz4_decomp_len] = decompressLZ4(
                lz4_raw_data, static_cast<int32_t>(compressed_block_size), decompress_buf_size
        );
        decompressed_data = lz4_decomp_data;
        actual_decompress_len = lz4_decomp_len;
        ASSERT_NE(decompressed_data, nullptr) << "LZ4 decompression failed (corrupted data)";
        ASSERT_GT(actual_decompress_len, 0) << "LZ4 decompression failed, code: " << actual_decompress_len;
    }

    std::cout << "Original data size: " << original_data.size() << std::endl;
    std::cout << "Decompressed data size: " << actual_decompress_len << std::endl;
    std::cout << "Decompressed data: " << std::string(decompressed_data, actual_decompress_len) << std::endl;

    ASSERT_EQ(static_cast<size_t>(actual_decompress_len), original_data.size())
                                << "Decompressed length mismatch! Expected: " << original_data.size()
                                << ", Actual: " << actual_decompress_len;
    ASSERT_EQ(std::string(decompressed_data, actual_decompress_len), original_data)
                                << "Decompressed content mismatch!";

    delete[] compressed_data;
    delete[] decompressed_data;
}