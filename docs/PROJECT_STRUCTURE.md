# Kiwi Project Structure

## Directory Organization

```
kiwi/
├── scripts/              # Development and build scripts
│   ├── dev.cmd          # Windows development helper
│   ├── dev.sh           # Linux/macOS development helper
│   ├── dev.ps1          # PowerShell development helper
│   ├── fast_build.sh    # Fast build script (Unix)
│   ├── fast_build.ps1   # Fast build script (Windows)
│   ├── setup_sccache.sh # sccache setup (Unix)
│   ├── setup_sccache.ps1# sccache setup (Windows)
│   └── README.md        # Scripts documentation
│
├── docs/                # Documentation
│   ├── QUICK_START.md   # Quick start guide
│   ├── BUILD_OPTIMIZATION.md # Build optimization guide
│   ├── 编译加速方案总结.md    # Chinese build guide
│   ├── 使用说明.txt          # Chinese usage guide
│   └── ...              # Other documentation
│
├── src/                 # Source code
│   ├── server/          # Server implementation
│   ├── net/             # Network layer
│   ├── storage/         # Storage layer
│   ├── raft/            # Raft consensus
│   ├── cmd/             # Command processing
│   ├── executor/        # Command executor
│   └── ...              # Other modules
│
├── .cargo/              # Cargo configuration
│   └── config.toml      # Build optimization settings
│
├── README.md            # Main README (English)
├── README_CN.md         # Main README (Chinese)
└── Cargo.toml           # Workspace configuration
```

## Quick Reference

### For Developers

**Windows:**
```cmd
scripts\dev check        # Quick syntax check
scripts\dev watch        # Auto-check on file save
scripts\dev run          # Build and run
```

**Linux/macOS:**
```bash
chmod +x scripts/*.sh    # First time only
./scripts/dev.sh check   # Quick syntax check
./scripts/dev.sh watch   # Auto-check on file save
./scripts/dev.sh run     # Build and run
```

**Cross-platform:**
```bash
cargo check              # Quick syntax check
cargo build              # Build project
cargo run                # Build and run
```

### Documentation

- **Getting Started**: [README.md](../README.md) or [README_CN.md](../README_CN.md)
- **Quick Start**: [docs/QUICK_START.md](QUICK_START.md)
- **Build Optimization**: [docs/BUILD_OPTIMIZATION.md](BUILD_OPTIMIZATION.md)
- **Scripts Usage**: [scripts/README.md](../scripts/README.md)
- **Chinese Guides**: [docs/编译加速方案总结.md](编译加速方案总结.md)

### Scripts

All development scripts are located in the `scripts/` directory:

- **dev**: Main development tool (Windows: `.cmd`, Unix: `.sh`, PowerShell: `.ps1`)
- **fast_build**: Optimized build script
- **setup_sccache**: Configure compilation caching

See [scripts/README.md](../scripts/README.md) for detailed usage.

## Key Features

### Cross-Platform Support

- Windows batch files (`.cmd`)
- Unix shell scripts (`.sh`)
- PowerShell scripts (`.ps1`)

### Build Optimization

- Incremental compilation enabled
- sccache support for caching
- Fast check mode for development
- Auto-watch mode for continuous checking

### Documentation

- English and Chinese documentation
- Quick start guides
- Detailed optimization guides
- Script usage documentation

## Contributing

When adding new scripts or documentation:

1. **Scripts**: Add to `scripts/` directory
   - Provide both Windows (`.cmd`) and Unix (`.sh`) versions
   - Update `scripts/README.md`

2. **Documentation**: Add to `docs/` directory
   - Provide English and Chinese versions when possible
   - Update main README files

3. **Configuration**: Update `.cargo/config.toml` for build settings

## See Also

- [Main README](../README.md)
- [中文 README](../README_CN.md)
- [Quick Start Guide](QUICK_START.md)
- [Build Optimization](BUILD_OPTIMIZATION.md)
- [Scripts Documentation](../scripts/README.md)
