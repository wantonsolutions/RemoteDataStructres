import log
import simulation_runtime as sr

def main():
    #test_hashes()
    logger = log.setup_custom_logger('root')
    logger.info("Starting simulator")
    config = sr.default_config()
    simulator = sr.Simulator(config)
    simulator.run()
    if not simulator.validate_run():
        logger.error("Simulation failed check the logs")
    simulator.collect_stats()

if __name__ == "__main__":
    main()