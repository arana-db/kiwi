// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Check if PROTOC is already set
    if std::env::var("PROTOC").is_ok() {
        println!("cargo:warning=Using PROTOC from environment variable");
    } else {
        // Try to use vendored protoc (automatically downloaded and used)
        match protoc_bin_vendored::protoc_bin_path() {
            Ok(protoc_path) => {
                std::env::set_var("PROTOC", protoc_path);
                println!("cargo:warning=Using vendored protoc");
            }
            Err(e) => {
                eprintln!("⚠️  protoc not found! Please install protoc:");
                eprintln!(
                    "     - Windows: Download from https://github.com/protocolbuffers/protobuf/releases"
                );
                eprintln!("     - Linux: sudo apt-get install protobuf-compiler");
                eprintln!("     - macOS: brew install protobuf");
                eprintln!("     Or set PROTOC environment variable to the protoc binary path");
                eprintln!();
                eprintln!("Error details: Failed to get vendored protoc path: {}", e);
                return Err("Could not find `protoc`. If `protoc` is installed, try setting the `PROTOC` environment variable to the path of the `protoc` binary. To install it on Debian, run `apt-get install protobuf-compiler`. It is also available at https://github.com/protocolbuffers/protobuf/releases  For more information: https://docs.rs/prost-build/#sourcing-protoc".to_string().into());
            }
        }
    }

    // Generate protobuf code
    prost_build::Config::new()
        .compile_protos(&["proto/binlog.proto"], &["proto/"])
        .map_err(|e| format!("Failed to compile protobuf: {}", e).into())
}
