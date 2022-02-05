#!/bin/env bash

cargo test --release
rm test*.db
