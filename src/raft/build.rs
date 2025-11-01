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
// WITHOUT WARRANTIES OR ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Generate protobuf code
    // Check if protoc is available
    match prost_build::Config::new().compile_protos(&["proto/binlog.proto"], &["proto/"]) {
        Ok(_) => Ok(()),
        Err(e) => {
            // If protoc is not found, provide helpful error message
            let error_msg = e.to_string();
            if error_msg.contains("protoc") || error_msg.contains("Could not find") {
                eprintln!("\n⚠️  protoc not found! Please install protoc:");
                eprintln!(
                    "   - Windows: Download from https://github.com/protocolbuffers/protobuf/releases"
                );
                eprintln!("   - Linux: sudo apt-get install protobuf-compiler");
                eprintln!("   - macOS: brew install protobuf");
                eprintln!("   Or set PROTOC environment variable to the protoc binary path\n");
            }
            Err(e)
        }
    }
}
