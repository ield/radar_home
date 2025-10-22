import asyncio
import csv
import time
from bleak import BleakScanner, BleakClient

# Configurable variables
# Seconds to update the file in which the data is written
FILE_INTERVAL = 10
# Beginning of the filename
FILEPATH_MEAS = "server_docs/meas/"
# Characteristic read and used for notifications
CHARACTERISTIC_UUID = "000000F1-8E22-4541-9D4C-21EDAE82ED19"

# Global Variables
# Cola asíncrona para notificaciones. No se usa con multiples nodos.
# Cada nodo tiene su propia cola
device_rssi = {}  # Almacena el RSSI obtenido en el escaneo

async def device_handler(dispositivo, log_filename, rssi):
    """
    Maneja la conexión y recepción de notificaciones de un dispositivo BLE,
    guardando los datos en un fichero CSV propio.
    """
    notification_queue = asyncio.Queue()

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
        
        # Extract the device name, needed to store the data
        device_name = selected_device.name


        # Extract starting the RSSI, common for all packets
        rssi = device_rssi.get(selected_device.address, None)
    
        """
        Data is stored in batches. Every BATCH_SIZE the data is flushed
        to a CSV file
        """
        batch = []
        BATCH_SIZE = 1000  # Escribiremos cada 1000 notificaciones
    
        batch = []
        BATCH_SIZE = 1000  # Escribe cada 1000 notificaciones
        
        # Start time for file rotation
        start_time = time.time()
        
        # Generate initial filename
        timestamp_str = time.strftime("%Y%m%d%H%M%S", time.localtime())
        log_filename = f"{FILEPATH_MEAS}{device_name}_{timestamp_str}.csv"
        
        # Open first file
        csvfile = open(log_filename, "w", newline="")
        writer = csv.writer(csvfile)
        writer.writerow(["Timestamp", "Sequence", "RSSI", "RawData"])
    
        try:
            while True:
                sequence, raw_data = await notification_queue.get()
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
                    log_filename = f"{FILEPATH_MEAS}{device_name}_{timestamp_str}.csv"

                    # Open new file
                    csvfile = open(log_filename, "w", newline="")
                    writer = csv.writer(csvfile)
                    writer.writerow(["Timestamp", "Sequence", "RSSI", "RawData"])
        except asyncio.CancelledError:
            if batch:
                writer.writerows(batch)
                csvfile.flush()
                
                # Close the current csv file
                csvfile.close()
            raise

    async with BleakClient(dispositivo.address) as client:
        await client.start_notify(CHARACTERISTIC_UUID, notification_handler)
        print(f"Conectado a {dispositivo.name} ({dispositivo.address}). Datos se guardarán en '{log_filename}'.")
        consumer_task = asyncio.create_task(process_notifications())
        try:
            while True:
                await asyncio.sleep(1)
                if packet_count >= 100000:
                    print(f"Se han recibido 100,000 paquetes de {dispositivo.name}. Deteniendo notificaciones...")
                    await client.stop_notify(CHARACTERISTIC_UUID)
                    break
        except KeyboardInterrupt:
            print(f"Desconectando {dispositivo.name} por interrupción de teclado...")
        finally:
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass
            print(f"Total de paquetes recibidos para {dispositivo.name}: {packet_count}")

async def run():
    # Se pide la distancia para incorporar en el nombre del fichero
    distancia = input("Ingresa la distancia a la que se ha medido: ").strip()

    print("Escaneando dispositivos BLE...")
    dispositivos = await BleakScanner.discover()

    if len(dispositivos) < 2:
        print("No se encontraron al menos dos dispositivos.")
        return

    # Se muestran los dispositivos encontrados
    for idx, dispositivo in enumerate(dispositivos):
        print(f"{idx}: {dispositivo.name} - {dispositivo.address} - RSSI: {dispositivo.rssi} dBm")

    indices_input = input("Ingresa los índices de los dos dispositivos a conectar, separados por coma: ")
    indices_str = indices_input.split(",")
    if len(indices_str) != 2:
        print("Se deben ingresar exactamente dos índices.")
        return

    try:
        index1 = int(indices_str[0].strip())
        index2 = int(indices_str[1].strip())
    except ValueError:
        print("Índices inválidos.")
        return

    if index1 < 0 or index1 >= len(dispositivos) or index2 < 0 or index2 >= len(dispositivos):
        print("Algún índice está fuera de rango.")
        return

    if index1 == index2:
        print("Los dos índices deben ser distintos.")
        return

    dispositivo1 = dispositivos[index1]
    dispositivo2 = dispositivos[index2]

    # Se crean nombres de archivo únicos para cada dispositivo (se limpia la dirección para formar el nombre)
    safe_address1 = dispositivo1.address.replace(":", "")
    safe_address2 = dispositivo2.address.replace(":", "")
    log_filename1 = f"hp_cw_dist_{distancia}m_{safe_address1}.csv"
    log_filename2 = f"hp_cw_dist_{distancia}m_{safe_address2}.csv"

    print("Conectando a dos dispositivos BLE...")

    task1 = asyncio.create_task(device_handler(dispositivo1, log_filename1, dispositivo1.rssi))
    task2 = asyncio.create_task(device_handler(dispositivo2, log_filename2, dispositivo2.rssi))

    await asyncio.gather(task1, task2)

if __name__ == "__main__":
    asyncio.run(run())

