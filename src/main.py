#!/usr/bin/env python3
"""
Beacon Chain Indexer
Simple ELT-based indexer for Ethereum beacon chain data.
"""
import asyncio
from src.cli import main

if __name__ == "__main__":
    asyncio.run(main())