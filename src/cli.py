import asyncio
import argparse
from src.services.loader import LoaderService
from src.services.transformer import TransformerService
from src.services.clickhouse import ClickHouse
from src.services.fork import ForkDetectionService
from src.utils.logger import setup_logger, logger
from src.config import config

def create_parser():
    """Create CLI argument parser."""
    parser = argparse.ArgumentParser(description="Beacon Chain Indexer")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Load command
    load_parser = subparsers.add_parser("load", help="Load raw data from beacon API")
    load_subparsers = load_parser.add_subparsers(dest="load_command")
    
    # Load realtime
    realtime_parser = load_subparsers.add_parser("realtime", help="Load new raw data continuously")
    
    # Load backfill  
    backfill_parser = load_subparsers.add_parser("backfill", help="Load historical raw data")
    backfill_parser.add_argument("--start-slot", type=int, default=config.START_SLOT, 
                                help=f"Start slot (default: {config.START_SLOT})")
    backfill_parser.add_argument("--end-slot", type=int, default=config.END_SLOT,
                                help=f"End slot (default: {config.END_SLOT or 'current head'})")
    
    # Transform command
    transform_parser = subparsers.add_parser("transform", help="Process raw data into structured tables")
    transform_subparsers = transform_parser.add_subparsers(dest="transform_command")
    
    # Transform run
    run_parser = transform_subparsers.add_parser("run", help="Run transformer continuously")
    run_parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing")
    
    # Transform reprocess
    reprocess_parser = transform_subparsers.add_parser("reprocess", help="Reprocess specific slot range")
    reprocess_parser.add_argument("--start-slot", type=int, default=config.START_SLOT,
                                 help=f"Start slot (default: {config.START_SLOT})")
    reprocess_parser.add_argument("--end-slot", type=int, default=config.END_SLOT,
                                 help=f"End slot (default: {config.END_SLOT or 'required'})")
    reprocess_parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing")
    
    # Fork command
    fork_parser = subparsers.add_parser("fork", help="Fork-related operations")
    fork_subparsers = fork_parser.add_subparsers(dest="fork_command")
    
    # Fork info
    info_parser = fork_subparsers.add_parser("info", help="Show fork information")
    info_parser.add_argument("--slot", type=int, help="Show fork for specific slot")
    info_parser.add_argument("--epoch", type=int, help="Show fork for specific epoch")
    
    # Fork list
    list_parser = fork_subparsers.add_parser("list", help="List all configured forks")
    
    return parser

async def main():
    """Main CLI entry point."""
    setup_logger()
    parser = create_parser()
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        if args.command == "load":
            await handle_load_command(args)
        elif args.command == "transform":
            await handle_transform_command(args)
        elif args.command == "fork":
            await handle_fork_command(args)
        else:
            parser.print_help()
    
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error("Application error", error=str(e))
        raise

async def handle_load_command(args):
    """Handle load command."""
    loader_service = LoaderService()
    
    try:
        await loader_service.initialize()
        
        if args.load_command == "realtime":
            await loader_service.realtime()
        
        elif args.load_command == "backfill":
            # Use provided args or fall back to config defaults
            start_slot = args.start_slot
            end_slot = args.end_slot
            
            # If end_slot is None, get current head slot
            if end_slot is None:
                head_slot = await loader_service.beacon_api.get_head_slot()
                if head_slot is None:
                    logger.error("Could not determine head slot and no end slot provided")
                    return
                end_slot = head_slot
                logger.info("Using current head slot as end slot", end_slot=end_slot)
            
            if start_slot >= end_slot:
                logger.error("Start slot must be less than end slot", 
                           start_slot=start_slot, end_slot=end_slot)
                return
                
            await loader_service.backfill(start_slot, end_slot)
        
        else:
            print("Usage: load {realtime|backfill}")
    
    finally:
        await loader_service.cleanup()

async def handle_transform_command(args):
    """Handle transform command."""
    transformer_service = TransformerService()
    
    if args.transform_command == "run":
        await transformer_service.run(args.batch_size)
    
    elif args.transform_command == "reprocess":
        start_slot = args.start_slot
        end_slot = args.end_slot
        
        if end_slot is None:
            logger.error("End slot is required for reprocessing")
            return
            
        if start_slot >= end_slot:
            logger.error("Start slot must be less than end slot",
                        start_slot=start_slot, end_slot=end_slot)
            return
            
        await transformer_service.reprocess(
            start_slot, 
            end_slot, 
            args.batch_size
        )
    
    else:
        print("Usage: transform {run|reprocess}")

async def handle_fork_command(args):
    """Handle fork command with auto-detection."""
    # Create fork service with auto-detection
    clickhouse = ClickHouse()
    fork_service = ForkDetectionService(clickhouse_client=clickhouse)
    
    if args.fork_command == "info":
        if args.slot is not None:
            fork_info = fork_service.get_fork_at_slot(args.slot)
            epoch = args.slot // fork_service.slots_per_epoch
            print(f"Slot {args.slot} (epoch {epoch}): {fork_info.name}")
            print(f"  Version: {fork_info.version}")
            print(f"  Activation Epoch: {fork_info.epoch}")
        
        elif args.epoch is not None:
            fork_info = fork_service.get_fork_at_epoch(args.epoch)
            print(f"Epoch {args.epoch}: {fork_info.name}")
            print(f"  Version: {fork_info.version}")
            print(f"  Activation Epoch: {fork_info.epoch}")
        
        else:
            print("Usage: fork info --slot <slot> or --epoch <epoch>")
    
    elif args.fork_command == "list":
        network_name = fork_service.get_network_name()
        network_suffix = " (auto-detected)" if fork_service.is_auto_detected() else " (unknown)"
        print(f"\nDetected network: {network_name}{network_suffix}")
        print("=" * 60)
        
        all_forks = fork_service.get_all_forks()
        if all_forks:
            for fork_name, fork_info in all_forks.items():
                print(f"{fork_name.upper():<12} | Epoch: {fork_info.epoch:<8} | Version: {fork_info.version}")
        else:
            print("No forks detected (run migration and load data first)")
        
        print(f"\nNetwork Configuration:")
        print(f"  Slots per epoch: {fork_service.slots_per_epoch}")
        print(f"  Seconds per slot: {fork_service.seconds_per_slot}")
        print(f"  Genesis time: {fork_service.genesis_time}")
    
    else:
        print("Usage: fork {info|list}")

if __name__ == "__main__":
    asyncio.run(main())