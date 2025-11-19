import asyncio
import csv
import os
import time
import struct
from bleak import BleakScanner, BleakClient

# Configurable variables
# Seconds to update the file in which the data is written
FILE_INTERVAL = 600
# Beginning of the filename
FILEPATH_MEAS = "meas/"
# Characteristic read and used for notifications
CHARACTERISTIC_UUID = "000000F1-8E22-4541-9D4C-21EDAE82ED19"
NAME_REF = "Rad_CW_C_"


# Function containing the discovery procedure
async def find_device():
    """Scan until the desired device is found."""
    print("Scanning for BLE devices...")
    while True:
        devices = await BleakScanner.discover(timeout=5.0)

        # Escoge el dispositivo indicado
        dispositivos_filtrados = [
            d for d in devices if d.name and NAME_REF in d.name
        ]
        if dispositivos_filtrados
            return dispositivos_filtrados

        await asyncio.sleep(3)


    

# Function containing all the connection procedure
async def connect_to_device(device):
    print(f"Conectando a {device.name} ({device.address})...")
    notification_queue = asyncio.Queue()
    handler = make_notification_handler(notification_queue)

    try:
        async with BleakClient(device.address) as client:
            await client.start_notify(CHARACTERISTIC_UUID, handler)
            print(f"Conectado a {device.name}")

            consumer_task = asyncio.create_task(process_notifications(device, notification_queue))
            try:
                while True:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                pass
            finally:
                consumer_task.cancel()
                await asyncio.sleep(0)
    except Exception as e:
        print(f"Error con {device.name}: {e}")

def make_notification_handler(notification_queue):        
    def notification_handler(sender: int, data: bytearray):
        """
        Handle incomming BLE data and enqueue it.
        Se asume que el buffer tiene 244 bytes:
          - Los primeros 240 bytes: datos en crudo (I y Q intercalados).
          - Últimos 4 bytes: número de secuencia (little endian).
          
        MODIFICACION: Se devuelven los datos directamente sin procesar
        para guardarlos en un fichero binario
        """
        if len(data) < 244:
            return

        # Encolamos los datos
        notification_queue.put_nowait(data)
    return notification_handler

async def process_notifications(selected_device, notification_queue):
    """
    Consume queued notifications and write them into bin files,
    rotating files every FILE_INTERVAL seconds.
    """
    # Extract the device name, needed to store the data
    device_name = selected_device.name

    """
    Data is stored in batches. Every BATCH_SIZE the data is flushed
    to a CSV file
    """
    batch = bytearray()
    BATCH_SIZE = 100  # Escribiremos cada 1000 notificaciones

    # Start time for file rotation
    start_time = time.time()
    
    # Generate initial filename
    timestamp_str = time.strftime("%Y%m%d%H%M%S", time.localtime())
    log_filename = f"{FILEPATH_MEAS}{device_name}_{timestamp_str}.bin"
    
    # Open first file
    bin_file = open(log_filename, "wb")
    
    try:
        while True:
            raw_data = await notification_queue.get()
            timestamp = int(time.time())
            
            #print(len(raw_data))
            # Pack into binary format
            ble_package = struct.pack("<I244s", timestamp, raw_data)
            batch.extend(ble_package)
            
            # Write the batch periodically
            if len(batch) >= BATCH_SIZE * 248:
                bin_file.write(batch)
                bin_file.flush()
                batch.clear()
                
            # Rotate file if interval passed
            if time.time() - start_time >= FILE_INTERVAL:
                # Flush the remaining data and clear the batch
                if batch:
                    bin_file.write(batch)
                    bin_file.flush()
                    batch.clear()
                
                # Close the current csv file
                bin_file.close()

                # Reset timer
                start_time = time.time()
                timestamp_str = time.strftime("%Y%m%d%H%M%S", time.localtime())
                log_filename = f"{FILEPATH_MEAS}{device_name}_{timestamp_str}.bin"

                # Open new file
                bin_file = open(log_filename, "wb")
                
    except asyncio.CancelledError:
        # Final flush, before exit, after the user cancels the execution
        print(f"Desconectando {device_name}")
            
        if batch:
            bin_file.write(batch)
            bin_file.flush()
            
        bin_file.close()

        await client.stop_notify(CHARACTERISTIC_UUID)

        raise

async def run():
    global packet_count

    # Launch all device connections concurrently 
    # with a small delay between connections
    tasks = []
    for i, device in enumerate(dispositivos_filtrados):
        task = asyncio.create_task(connect_to_device(device))
        tasks.append(task)
        await asyncio.sleep(10)  # small delay before starting next connection

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        # Close all tasks
        for t in tasks:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        print("Dispositivos desconectados. Fin del programa")


if __name__ == "__main__":
    asyncio.run(run())
