//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

#include <immintrin.h>
#include <stdint.h>

void hamming_byte_256(unsigned char *a, unsigned char *b, unsigned int *res, long *len)
{
    int n = *len;

    // fast path for small dimensions
    if (n < 32)
    {
        long acc = 0;
        for (int i = 0; i < n; i++)
        {
            acc += a[i] != b[i] ? 1 : 0;
        }

        *res = acc;
        return;
    }

    __m256i acc = _mm256_setzero_si256();

    int i;
    // Process 32 bytes at a time
    for (i = 0; i + 31 < n; i += 32)
    {
        __m256i vec_a, vec_b;

        // Load 32 bytes
        vec_a = _mm256_loadu_si256((const __m256i *)(a + i));
        vec_b = _mm256_loadu_si256((const __m256i *)(b + i));

        // Create two registries for vector a
        __m256i a_high = _mm256_srai_epi16(vec_a, 8);  // arithmetic right shift sign extends
        __m256i a_low = _mm256_bslli_epi128(vec_a, 1); // left 1 byte = low to high in each 16-bit element
        a_low = _mm256_srai_epi16(a_low, 8);           // arithmetic right shift sign extends

        // Create two registries for vector b
        __m256i b_high = _mm256_srai_epi16(vec_b, 8);
        __m256i b_low = _mm256_bslli_epi128(vec_b, 1);
        b_low = _mm256_srai_epi16(b_low, 8);

        // Multiply packed signed 16 - bit integers in a and b, producing intermediate signed 32 - bit integers.Horizontally add adjacent pairs of intermediate 32 - bit integers, and pack the results in dst.

        __m256i cmp_result_high = _mm256_cmpeq_epi16(a_high, b_high);
        __m256i cmp_result_low = _mm256_cmpeq_epi16(a_low, b_low);

        cmp_result_high = _mm256_srli_epi16(cmp_result_high, 15);
        cmp_result_low = _mm256_srli_epi16(cmp_result_low, 15);

        __m256i sum = _mm256_add_epi16(cmp_result_low, cmp_result_high);

        __m128i sum_lower = _mm256_extracti128_si256(sum, 0);
        __m128i sum_upper = _mm256_extracti128_si256(sum, 1);
        __m256i sum_epi32 = _mm256_cvtepi16_epi32(sum_lower);

        sum_epi32 = _mm256_add_epi32(sum_epi32, _mm256_cvtepi16_epi32(sum_upper));

        acc = _mm256_add_epi32(acc, sum_epi32);
    }

    // Reduce
    __m128i acc_low = _mm256_extracti128_si256(acc, 0);
    __m128i acc_high = _mm256_extracti128_si256(acc, 1);
    __m128i acc128 = _mm_add_epi16(acc_low, acc_high);
    // Perform horizontal adds by shuffling and adding
    acc128 = _mm_add_epi16(acc128, _mm_shuffle_epi32(acc128, _MM_SHUFFLE(1, 0, 3, 2)));
    acc128 = _mm_add_epi16(acc128, _mm_shuffle_epi32(acc128, _MM_SHUFFLE(2, 3, 0, 1)));
    acc128 = _mm_add_epi16(acc128, _mm_shufflelo_epi16(acc128, _MM_SHUFFLE(0, 1, 2, 3)));
    acc128 = _mm_add_epi16(acc128, _mm_srli_si128(acc128, 2)); // Shift and add to accumulate upper parts
    acc128 = _mm_add_epi16(acc128, _mm_srli_si128(acc128, 4)); // Continue shifting and adding

    // At this point, the sum should be in the lowest 16-bit of the vector.
    int result = _mm_extract_epi16(acc128, 0); // Extract the 16-bit sum

    // Tail
    for (; i < n; i++)
    {
        result += a[i] != b[i] ? 1 : 0;
    }

    *res = result;
}