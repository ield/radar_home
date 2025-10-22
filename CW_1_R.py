import asyncio
import csv
import os
import time
from bleak import BleakScanner, BleakClient

# Configurable variables
# Seconds to update the file in which the data is written
FILE_INTERVAL = 10
# Beginning of the filename
FILENAME_REF = "server_docs/meas/name"
# Characteristic read and used for notifications
CHARACTERISTIC_UUID = "000000F1-8E22-4541-9D4C-21EDAE82ED19"

# Global Variables
# Archivo de log y contador global
packet_count = 0
# Cola asíncrona para notificaciones
notification_queue = asyncio.Queue()
device_rssi = {}  # Almacena el RSSI obtenido en el escaneo

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
    sequence = int.from_bytes(data[-2:], byteorder="little")
    sequence = int((sequence + 1) / 30)
    # Encolamos la tupla (sequence, raw_data)
    notification_queue.put_nowait((sequence, raw_data))

async def process_notifications(selected_device):
    """
    Consume queued notifications and write them into CSV files,
    rotating files every FILE_INTERVAL seconds.
    """
    global packet_count
    
    # Extract starting the RSSI, common for all packets
    rssi = device_rssi.get(selected_device.address, None)
    
    """
    Data is stored in batches. Every BATCH_SIZE the data is flushed
    to a CSV file
    """
    batch = []
    BATCH_SIZE = 1000  # Escribiremos cada 1000 notificaciones

    # Start time for file rotation
    start_time = time.time()
    
    # Generate initial filename
    timestamp_str = time.strftime("%Y%m%d%H%M%S", time.localtime())
    log_filename = f"{FILENAME_REF}_{timestamp_str}.csv"
    
    # Open first file
    csvfile = open(log_filename, "w", newline="")
    writer = csv.writer(csvfile)
    writer.writerow(["Timestamp", "Sequence", "RSSI", "RawData"])

    try:
        while True:
            sequence, raw_data = await notification_queue.get()
            packet_count += 1
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            raw_str = ';'.join(str(b) for b in raw_data)
            batch.append([timestamp, sequence, rssi, raw_str])
            
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
                log_filename = f"{FILENAME_REF}_{timestamp_str}.csv"

                # Open new file
                csvfile = open(log_filename, "w", newline="")
                writer = csv.writer(csvfile)
                writer.writerow(["Timestamp", "Sequence", "RSSI", "RawData"])
                
    except asyncio.CancelledError:
        # Final flush, before exit, after the user cancels the execution
        if batch:
            writer.writerows(batch)
            csvfile.flush()
            
        csvfile.close()
        raise

async def run():
    global packet_count
    NAME_REF = "Rad_CW_L_F1"
        
    print("Escaneando dispositivos BLE...")
    dispositivos = await BleakScanner.discover()
    
    if not dispositivos:
        print("No se encontraron dispositivos.")
        return

    # Escoge el dispositivo indicado
    dispositivo_seleccionado = None
    for idx, dispositivo in enumerate(dispositivos):
        if dispositivo.name == NAME_REF:
            dispositivo_seleccionado = dispositivo

    if not dispositivo_seleccionado:
        print("Dispositivo no encontrado")
        return

    print("Conectando a:", dispositivo_seleccionado.name, dispositivo_seleccionado.address)

    try:
        async with BleakClient(dispositivo_seleccionado.address) as client:
            await client.start_notify(CHARACTERISTIC_UUID, notification_handler)
            print(f"Conectado.")
            
            # Iniciamos la tarea para procesar notificaciones
            consumer_task = asyncio.create_task(process_notifications(dispositivo_seleccionado))
            
            try:
                # Keep loop alive
                while True:
                    await asyncio.sleep(1)
            # Stop if there is a keyboard interruption        
            except KeyboardInterrupt:
                print("Desconectando por interrupción de teclado...")
            
            # Cancel all tasks after code ends
            finally:
                consumer_task.cancel()
                try:
                    await consumer_task
                except asyncio.CancelledError:
                    pass
    except Exception as e:
        print(f"Error: {e}")

    print(f"Total de paquetes recibidos: {packet_count}")

if __name__ == "__main__":
    asyncio.run(run())
