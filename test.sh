#!/bin/env bash

cargo test --release -- --test-threads 1
rm test*.db
