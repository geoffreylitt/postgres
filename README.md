PostgreSQL with BSON support
============================

This is a fork of the PostgreSQL database which adds a BSON data type. BSON stores JSON documents in a binary format that can be queried more efficiently than JSON text.

Developed for CS438 @ Yale by Geoffrey Litt, Seth Thompson, and John Whittaker.

Installation
------------

To install, you need the [mongo-c-driver](https://github.com/mongodb/mongo-c-drive) and [json-c](https://github.com/json-c/json-c) libraries installed. Make sure that you've followed the install directions for each library so that they're compiled fully. Use mongo-c-driver v0.7.1 and json-c v0.11. Newer/older versions of the libraries may not work. (Note: on the Zoo, you may have to specify a `--prefix=/path/to/dir` option to the configure scripts for the libraries, if you don't have permission to install to standard directories.)

You can set environment variables to tell Postgres where to find the libraries, and what directory to install binaries to:

    MONGO_C_DIR=/path/to/mongo-c-driver
    JSON_C_DIR=/path/to/json-c
    BUILD_DIR=/path/to/install/dir

And then configure the Postgres make using those environment variables by running this command:

    ./configure --prefix=$BUILD_DIR --enable-debug --enable-cassert CFLAGS="--std=c99 -I$MONGO_C_DIR/src -I$JSON_C_DIR/include" LDFLAGS="-L$MONGO_C_DIR -Wl,-rpath=$MONGO_C_DIR -L$JSON_C_DIR/lib64 -Wl,-rpath=$JSON_C_DIR/lib64" LIBS="-lbson -lmongoc -ljson-c"

And then install as usual:

    make
    make install

Notes
-----

- If you get an error about not finding `libbson.so.0.7`, try navigating to the mongo-c-driver directory, which should contain `libbson.so`, and create a copy of that file which is called `libbson.so.0.7`.