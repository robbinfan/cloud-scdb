#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>

#include "../include/scdb/scdb.h"
#include "../src/utils/timestamp.h"

#include "cmdopt.h"

#include <glog/logging.h>

namespace {

void print_help(const char *cmd) 
{
  std::cerr << "Usage: " << cmd << " [OPTION]... [FILE]...\n\n"
      "Options:\n"
      "  -w, --with-checksum    build a dictionary with checksum\n"
      "  -i, --input=[FILE]     read data to FILE\n"
      "  -o, --output=[FILE]    write data to FILE\n"
      "  -t, --tmpdir=[FILE]    tmp dir to store tmp file \n"
      "  -h, --help             print this help\n"
      << std::endl;
}

int build(const char* input, const char* output, const scdb::Writer::Option& opt)
{
    if (!input)
    {
        std::cerr << "no input!!!" << std::endl;
        exit(-1);
    }

    scdb::Timestamp start(scdb::Timestamp::Now());
    scdb::Writer* writer = scdb::CreateWriter(opt, output);

    std::ifstream is(input);
    while (!is.eof())
    {
        char buf[4096];
        is.getline(buf, sizeof buf);
        writer->Put(buf);
    }
    writer->Close();
    delete writer;
    LOG(INFO) << "Build use " << scdb::Timestamp::Now().MicroSecondsSinceEpoch() - start.MicroSecondsSinceEpoch() << " microseconds";
    return 0;
}

}  // namespace

int main(int argc, char *argv[]) 
{
    std::ios::sync_with_stdio(false);

    ::cmdopt_option long_options[] = {
        { "with-checksum", 0, NULL, 'w' },
        { "input", 1, NULL, 'i'},
        { "output", 1, NULL, 'o' },
        { "tmpdir", 1, NULL, 't' },
        { "help", 0, NULL, 'h' },
        { NULL, 0, NULL, 0 }
    };
    ::cmdopt_t cmdopt;
    ::cmdopt_init(&cmdopt, argc, argv, "wi:o:t:h", long_options);

    scdb::Writer::Option opt;
    opt.build_type = 1;
    opt.compress_type = 0;

    int label;
    char* input = NULL;
    char* output = NULL;
    while ((label = ::cmdopt_get(&cmdopt)) != -1) {
        switch (label) {
            case 'w':
            {
                opt.with_checksum = true;
                break;
            }
            case 'i':
            {
                input = cmdopt.optarg;
                break;
            }
            case 'o': 
            {
                output = cmdopt.optarg;
                break;
            }
            case 't':
            {
                opt.temp_folder = cmdopt.optarg;
                if (opt.temp_folder[opt.temp_folder.length()-1] != '/')
                    opt.temp_folder.append("/");
                break;
            }
            case 'h': 
            {
                print_help(argv[0]);
                return 0;
            }
            default: 
            {
                return 1;
            }
        }
    }

    return build(input, output, opt);
}
