import asyncio
import csv
import os
import time
from bleak import BleakScanner, BleakClient

# Configurable variables
# Seconds to update the file in which the data is written
FILE_INTERVAL = 10
# Beginning of the filename
FILEPATH_MEAS = "server_docs/meas/"
# Characteristic read and used for notifications
CHARACTERISTIC_UUID = "000000F1-8E22-4541-9D4C-21EDAE82ED19"
# Subname containing all devices need to connect to
NAME_REF = "Rad_FM_L_"
# Number of FFT-bins in a BLE package
N_BIN_CHIRP = 3
N_CHIRP_PACKAGE = 60/N_BIN_CHIRP


# Global Variables
# Cola asíncrona para notificaciones. No se usa con multiples nodos.
# Cada nodo tiene su propia cola
device_rssi = {}  # Almacena el RSSI obtenido en el escaneo

async def run_device(device):
    """
    Conecta a un dispositivo BLE, inicia la notificación, procesa y guarda
    los datos en un archivo CSV específico para ese dispositivo.
    """
    print(f"Conectando a {device.name}")
    notification_queue = asyncio.Queue()
    rssi = 0  # RSSI obtenido durante el escaneo
    # Packet count to know when to flush the data
    packet_count = 0
    
    def notification_handler(sender: int, data: bytearray):
        """
        Handle incomming BLE data and enqueue it.
        Se asume que el buffer tiene 244 bytes:
          - Los primeros 240 bytes: datos en crudo (I y Q intercalados).
          - Últimos 4 bytes: número de secuencia (little endian).
        """
        if len(data) < 244:
            return

        raw_data = data[:240]
        # Se extrae la secuencia completa de los últimos 4 bytes en little-endian
        tail_bytes = data[-4:]
        sequence = int.from_bytes(tail_bytes[0:2], byteorder="little")
        bin_val = int.from_bytes(tail_bytes[2:4], byteorder="little")
        sequence = int((sequence + 1) / N_CHIRP_PACKAGE)
        # Encolamos la tupla (sequence, raw_data)
        notification_queue.put_nowait((sequence, bin_val, raw_data))
        
    async def process_notifications():
        """
        Consume queued notifications and write them into CSV files,
        rotating files every FILE_INTERVAL seconds.
        """
        """
        Data is stored in batches. Every BATCH_SIZE the data is flushed
        to a CSV file
        """
        batch = []
        BATCH_SIZE = 1000  # Escribiremos cada 1000 notificaciones

        # Start time for file rotation
        start_time = time.time()
        
        # Generate initial filename
        device_name = device.name
        timestamp_str = time.strftime("%Y%m%d%H%M%S", time.localtime())
        log_filename = f"{FILEPATH_MEAS}{device_name}_{timestamp_str}.csv"
        
        # Open first file
        csvfile = open(log_filename, "w", newline="")
        writer = csv.writer(csvfile)
        writer.writerow(["Timestamp", "Sequence", "Bin", "RSSI", "RawData"])

        try:
            while True:
                sequence, bin_val, raw_data = await notification_queue.get()
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                raw_str = ';'.join(str(b) for b in raw_data)
                batch.append([timestamp, sequence, bin_val, rssi, raw_str])
                
                # Write the batch periodically
                if len(batch) >= BATCH_SIZE:
                    writer.writerows(batch)
                    csvfile.flush()
                    batch.clear()
                    
                # Rotate file if interval passed
                if time.time() - start_time >= FILE_INTERVAL:
                    # Flush the remaining data and clear the batch
                    if batch:
                        writer.writerows(batch)
                        csvfile.flush()
                        batch.clear()
                    
                    # Close the current csv file
                    csvfile.close()

                    # Reset timer
                    start_time = time.time()
                    timestamp_str = time.strftime("%Y%m%d%H%M%S", time.localtime())
                    log_filename = f"{FILEPATH_MEAS}{device_name}_{timestamp_str}.csv"

                    # Open new file
                    csvfile = open(log_filename, "w", newline="")
                    writer = csv.writer(csvfile)
                    writer.writerow(["Timestamp", "Sequence", "Bin", "RSSI", "RawData"])
                    
        except asyncio.CancelledError:
            # Final flush, before exit, after the user cancels the execution
            if batch:
                writer.writerows(batch)
                csvfile.flush()
                
            csvfile.close()
            raise
    
    async with BleakClient(device.address) as client:
        await client.start_notify(CHARACTERISTIC_UUID, notification_handler)
        print(f"Conectado a {device.name}. Recibiendo datos...")

        consumer_task = asyncio.create_task(process_notifications())
        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            print(f"Desconectando {device.name} por interrupción de teclado...")
            await client.stop_notify(CHARACTERISTIC_UUID)
            pass
        finally:
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass

async def run():
    print("Escaneando dispositivos BLE...")
    dispositivos = await BleakScanner.discover()
    
    if not dispositivos:
        print("No se encontraron dispositivos.")
        return

    # Escoge el dispositivo indicado
    dispositivos_filtrados = [
        d for d in dispositivos if d.name and NAME_REF in d.name
    ]

    if not dispositivos_filtrados:
        print("Dispositivo no encontrado")
        return

    # Launch all device connections concurrently 
    # with a small delay between connections
    tasks = []
    for i, device in enumerate(dispositivos_filtrados):
        task = asyncio.create_task(run_device(device))
        tasks.append(task)
        await asyncio.sleep(2)  # small delay before starting next connection

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        print(f"Finalizado")
        pass

if __name__ == "__main__":
    asyncio.run(run())
