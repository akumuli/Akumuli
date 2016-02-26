find . -maxdepth 3 -name '*.cpp' -or -name '*.h' -exec clang-format-3.8 -i {} \;
