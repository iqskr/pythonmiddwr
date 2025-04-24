import asyncio
from transit_system import TransitSystem
from http_handlers import start_http_server

async def main():
    system = TransitSystem()
    await asyncio.gather(
        system.maintain_gtfs(),
        system.process_vehicles(),
        start_http_server(system)
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Process interrupted.")
