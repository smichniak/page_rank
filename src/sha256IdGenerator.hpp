#ifndef SRC_SHA256IDGENERATOR_HPP_
#define SRC_SHA256IDGENERATOR_HPP_

#include "immutable/idGenerator.hpp"
#include "immutable/pageId.hpp"

class Sha256IdGenerator : public IdGenerator {
public:
    virtual PageId generateId(std::string const& content) const {
        int static const HASH_LENGTH = 65;
        char const READ_MODE[2] = "r";
        char hash_buffer[HASH_LENGTH];

        std::string hash_command = "echo -n \"" + content + "\" | sha256sum";
        FILE* stream = popen(hash_command.c_str(), READ_MODE);
        if (!stream) {
            exit(1); // Failed to open pipe to to the other process
        }

        if (!fgets(hash_buffer, HASH_LENGTH, stream)) {
            pclose(stream);
            exit(1); // Failed to read the hash from the stream
        }

        pclose(stream);

        return PageId(hash_buffer);
    }
};

#endif /* SRC_SHA256IDGENERATOR_HPP_ */
