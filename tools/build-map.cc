#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>

#include <boost/algorithm/string.hpp>

#include "../include/scdb/scdb.h"
#include "../src/utils/timestamp.h"

#include "cmdopt.h"

#include <glog/logging.h>

namespace {

void print_help(const char *cmd) 
{
  std::cerr << "Usage: " << cmd << " [OPTION]... [FILE]...\n\n"
      "Options:\n"
      "  -c, --compress-snappy  build a dictionary with snappy compressed value(default not)\n"
      "  -d, --compress-dfa     build a dictionary with dfa compressed value(default not)\n"
      "  -w, --with-checksum    build a dictionary with checksum\n"
      "  -i, --input=[FILE]     read data to FILE\n"
      "  -o, --output=[FILE]    write data to FILE\n"
      "  -t, --tmpdir=[FILE]    tmp dir to store tmp file \n"
      "  -f  --fulltest         fulltest after build\n"
      "  -h, --help             print this help\n"
      << std::endl;
}

int build(const char* input, const char* output, const scdb::Writer::Option& opt, bool fulltest)
{
    if (!input)
    {
        std::cerr << "no input!!!" << std::endl;
        exit(-1);
    }

    scdb::Timestamp start(scdb::Timestamp::Now());
    scdb::Writer* writer = scdb::CreateWriter(opt, output);

    std::vector<std::string> vt;
    std::ifstream is(input);
    while (!is.eof())
    {
        char buf[4096];
        is.getline(buf, sizeof buf);
        if (strlen(buf) == 0)
            continue;

        if (opt.compress_type == 2)
        {
            writer->Put(buf);
            if (fulltest)
                vt.push_back(buf);
        }
        else
        {
            std::vector<std::string> v;
            boost::algorithm::split(v, buf, boost::is_any_of("\t"));
            writer->Put(v[0], v[1]);

            if (fulltest)
                vt.push_back(v[0]);
        }
    }
    writer->Close();
    delete writer;
    LOG(INFO) << "Build use " << scdb::Timestamp::Now().MicroSecondsSinceEpoch() - start.MicroSecondsSinceEpoch() << " microseconds";

    if (fulltest)
    {
        LOG(INFO) << "Full Test Run";
        scdb::Reader* reader = scdb::CreateReader(scdb::Reader::Option(), output);
        for (auto& ti : vt)
        {
            bool exist = reader->Exist(ti);
            CHECK(exist) << "Unexpect result " << ti << " should in sets";
        }
        LOG(INFO) << "Full Test Pass!!!";
    }
    return 0;
}

}  // namespace

int main(int argc, char *argv[]) 
{
    std::ios::sync_with_stdio(false);

    ::cmdopt_option long_options[] = {
        { "compress-snappy", 0, NULL, 'c' },
        { "compress-trie", 0, NULL, 'd' },
        { "with-checksum", 0, NULL, 'w' },
        { "input", 1, NULL, 'i'},
        { "output", 1, NULL, 'o' },
        { "tmpdir", 1, NULL, 't' },
        { "help", 0, NULL, 'h' },
        { "fulltest", 0, NULL, 'f' },
        { NULL, 0, NULL, 0 }
    };
    ::cmdopt_t cmdopt;
    ::cmdopt_init(&cmdopt, argc, argv, "fcdwi:o:t:h", long_options);

    scdb::Writer::Option opt;
    opt.build_type = 0;
    opt.compress_type = 0;

    bool fulltest = false;

    int label;
    char* input = NULL;
    char* output = NULL;
    while ((label = ::cmdopt_get(&cmdopt)) != -1) {
        switch (label) {
            case 'c': 
            {
                opt.compress_type = 1;
                break;
            }
            case 'd':
            {
                opt.compress_type = 2;
                break;
            }
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
            case 'f':
            {
                fulltest = true;
                break;
            }
            default: 
            {
                return 1;
            }
        }
    }

    return build(input, output, opt, fulltest);
}
