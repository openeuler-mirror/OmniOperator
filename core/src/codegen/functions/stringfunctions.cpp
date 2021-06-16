#include <iostream>
#include <string>
#include <cstring>
#include <memory>
#include <vector>
#include <cassert>
#include <algorithm>
#include <regex>
// Need to install re2 library from github.com/google/re2/wiki/Install and run "sudo ldconfig"
// #include <re2/re2.h>
// #include <re2/stringpiece.h>

#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

using namespace std;


extern "C" DLLEXPORT bool strEqualsExt(int64_t Ap, int64_t Bp) {
    char* A = (char*) Ap;
    char* B = (char*) Bp;

	int i = 0;
	bool ret = true;
	while (A[i] != '\0' || B[i] != '\0') {
		if (A[i] != B[i]) {
			ret = false;
            break;
		}
        i++;
	}

    int a = 0;
    while (A[a] != '\0') {
        a++;
    }
    int b = 0;
    while (B[b] != '\0') {
        b++;
    }

	return ret;
}


extern "C" DLLEXPORT int32_t strCompareExt(int64_t Ap, int64_t Bp) {
    char* A = (char*) Ap;
    char* B = (char*) Bp;
    string As = string(A);
    string Bs = string(B);
    // return sign(Bs - As), more or less
    if (As < Bs) return -1;
    if (As > Bs) return 1;
    return 0;
}


extern "C" DLLEXPORT bool likeExt(int64_t str, int64_t regexToMatch) {
    char* Sp = (char*) str;
    char* Rp = (char*) regexToMatch;
    string S = string(Sp);
    string R = string(Rp);
    // Using re2 library
    // return RE2::FullMatch(S, R);

    // Using std regex library
    regex Re = regex(R);
    return regex_match(S, Re);
}


extern "C" DLLEXPORT int64_t substrExt(int64_t str, int32_t startIdx, int32_t length) {
    char* s = (char*) str;
	char *ret = reinterpret_cast<char*>(malloc(length + 1));
	for (int i = 0; i < length; i++) {
		ret[i] = s[startIdx + i];
	}
	ret[length] = '\0';
	return (int64_t)(ret);
}


extern "C" DLLEXPORT int64_t concatStrExt(int64_t Ap, int64_t Bp) {
    char* A = (char*) Ap;
    char* B = (char*) Bp;
    string As = string(A);
    string Bs = string(B);
    char *ret = reinterpret_cast<char*>(malloc(As.size() + Bs.size() + 1));
    for (int i = 0; i < As.size(); i++) {
        ret[i] = As[i];
    }
    for (int j = 0; j < Bs.size(); j++) {
        ret[j + As.size()] = Bs[j];
    }
    ret[As.size() + Bs.size()] = '\0';
    return (int64_t)(ret);
}

extern "C" DLLEXPORT int32_t cast_string(int64_t str) {
    // Date is in the format 1996-02-28
    // Doesn't account for leap seconds or daylight savings
    // Should be ok just for dates 
    char* s = (char*) str;
    // std::cout << "original date: " << s << endl;
    int yr = 1000 * (s[0] - '0') + 100 * (s[1] - '0') + 10 * (s[2] - '0') + (s[3] - '0');
    int mnth = 10 * (s[5] - '0') + (s[6] - '0');
    int day = 10 * (s[8] - '0') + (s[9] - '0');

    struct std::tm epoch = {0, 0, 0, 1, 1, 70};
    struct std::tm t = {0, 0, 0, day, mnth, yr - 1900};
    std::time_t epochTime = std::mktime(&epoch);
    std::time_t desiredTime = std::mktime(&t);

    // std::cout << std::ctime(&epochTime);
    // std::cout << std::ctime(&desiredTime);

    int32_t ret = (int32_t) (std::difftime(desiredTime, epochTime) / 86400.0);

    // std::cout << "double date diff: " << std::difftime(desiredTime, epochTime) / 86400.0 << endl;
    // std::cout << "date diff: " << ret << " days" << endl;
    return ret;
}