    try:
        success = etl.run_pipeline(layer=args.layer, mode=args.mode, full_refresh=args.full_refresh)
        
        if success:
            logger.info("=" * 80)
            logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
        else:
            logger.error("=" * 80)
            logger.error("PIPELINE FAILED")
            logger.error("=" * 80)
            sys.exit(1)
            
    except Exception as e:
        logger.error("=" * 80)
        logger.error("PIPELINE FAILED")
        logger.error("=" * 80)
        logger.error(f"Error: {e}")
        logger.error("=" * 80)
        logger.exception("Full traceback:")
        sys.exit(1)
