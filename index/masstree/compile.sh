./bootstrap.sh
./configure --disable-assertions --with-malloc=tcmalloc

make -j64
make mtIndexAPI.a
