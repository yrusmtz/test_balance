import sqlite3
from datetime import datetime, timedelta
import random
import logging
import pandas as pd

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s'
    )


def generar_datos(num_transacciones):
    inicio_fecha = datetime(2022, 1, 1)
    fin_fecha = datetime(2023, 1, 1)
    datos = []
    id_transaccion = 1
    contador_transacciones = 0

    while contador_transacciones < num_transacciones:
        fecha = inicio_fecha + timedelta(
            days=random.randint(0, (fin_fecha - inicio_fecha).days)
            )
        credito = round(random.uniform(100, 10000), 2)
        num_debitos = random.randint(1, 5)
        valor_base_debito = credito / num_debitos
        suma_debitos = 0

        for _ in range(num_debitos - 1):
            variacion = random.uniform(-10, 10)
            debito = round(valor_base_debito + variacion, 2)
            suma_debitos += debito
            datos.append(
                (id_transaccion, fecha.strftime("%Y-%m-%d"), debito, 0)
                )
            id_transaccion += 1

        ultimo_debito = round(credito - suma_debitos, 2)
        datos.append(
            (id_transaccion, fecha.strftime("%Y-%m-%d"), ultimo_debito, 0)
            )
        id_transaccion += 1

        datos.append((id_transaccion, fecha.strftime("%Y-%m-%d"), 0, credito))
        id_transaccion += 1
        contador_transacciones += 1

    fecha = fin_fecha - timedelta(days=1)
    debito = sum(d[2] for d in datos)
    credito = sum(d[3] for d in datos)
    datos.append(
        (id_transaccion,
         fecha.strftime("%Y-%m-%d"),
         (debito - credito) if debito > credito else 0,
         (credito - debito) if credito > debito else 0)
        )
    return datos


def tabla_de_prueba():
    # Conectar a la base de datos SQLite
    conn = sqlite3.connect('transacciones.db')
    c = conn.cursor()

    # Crear tabla
    c.execute(
        '''CREATE TABLE IF NOT EXISTS transacciones
                         (id_transaccion INTEGER PRIMARY KEY, fecha TEXT, debito REAL, credito REAL)'''
        )

    # Borrar datos existentes
    c.execute('DELETE FROM transacciones')

    # Generar datos
    datos = generar_datos(10)

    # Insertar datos en la tabla
    c.executemany('INSERT INTO transacciones VALUES (?,?,?,?)', datos)

    # Guardar (commit) los cambios y cerrar la conexión
    conn.commit()
    conn.close()

    def obtener_creditos_y_debitos():
        conn = sqlite3.connect('transacciones.db')
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id_transaccion, debito FROM transacciones WHERE debito > 0"
            )
        debitos = cursor.fetchall()
        cursor.execute(
            "SELECT id_transaccion, credito FROM transacciones WHERE credito > 0"
            )
        creditos = cursor.fetchall()
        conn.close()
        return creditos, debitos


def obtener_datos_de_excel():
    df = pd.read_excel('transacciones.xlsx')
    df = df.rename(
        columns={'TRAACR': 'id_transaccion', 'Db': 'debito', 'Cr': 'credito'}
        )
    df = df[['id_transaccion', 'debito', 'credito']]
    df['id_transaccion'] = df['id_transaccion'].astype(str)
    creditos = df[df['credito'] > 0][
        ['id_transaccion', 'credito']].values.tolist()
    debitos = df[df['debito'] > 0][['id_transaccion', 'debito']].values.tolist()
    return creditos, debitos


def encontrar_combinacion_optima(
        debitos,
        credito,
        start=0,
        current_sum=0,
        current_comb=[],
        tolerancia=0.01
        ):
    logging.debug(
        f"Iniciando encontrar_combinacion_optima con credito={credito}, start={start}, current_sum={current_sum}, "
        f"tolerancia={tolerancia}"
        )
    if abs(current_sum - credito) <= tolerancia:
        logging.debug(f"Combinación encontrada: {current_comb}")
        return current_comb
    if current_sum > credito or start >= len(debitos):
        return None
    incluir = encontrar_combinacion_optima(
        debitos, credito, start + 1, current_sum + debitos[start][1],
                          current_comb + [debitos[start]], tolerancia=tolerancia
        )
    if incluir is not None:
        return incluir
    excluir = encontrar_combinacion_optima(
        debitos, credito, start + 1, current_sum, current_comb, tolerancia=tolerancia
        )
    return excluir


def backtracking_combinacion_optima(
        debitos,
        credito,
        start=0,
        current_sum=0,
        current_comb=[],
        tolerancia=0.01
        ):
    if abs(current_sum - credito) <= tolerancia:
        return current_comb
    if current_sum > credito or start >= len(debitos):
        return None
    for i in range(start, len(debitos)):
        incluir = backtracking_combinacion_optima(
            debitos, credito, i + 1, current_sum + debitos[i][1],
                              current_comb + [debitos[i]], tolerancia=tolerancia
            )
        if incluir is not None:
            return incluir
    return None


def emparejar_creditos_debitos():
    logging.info("Iniciando emparejar_creditos_debitos")
    creditos, debitos = obtener_datos_de_excel()
    emparejamientos = []
    for credito in creditos:
        logging.debug(f"Procesando credito: {credito}")
        combinacion = encontrar_combinacion_optima(debitos, credito[1])
        if combinacion:
            emparejamientos.append((credito, combinacion))
            for debito in combinacion:
                debitos.remove(debito)
    logging.info(
        f"Emparejamientos completados: {len(emparejamientos)} encontrados"
        )
    return emparejamientos


def emparejar_creditos_debitos_backtracking():
    logging.info("Iniciando emparejar_creditos_debitos con backtracking")
    creditos, debitos = obtener_datos_de_excel()
    emparejamientos = []
    for credito in creditos:
        combinacion = backtracking_combinacion_optima(debitos, credito[1])
        if combinacion:
            emparejamientos.append((credito, combinacion))
            for debito in combinacion:
                debitos.remove(debito)
    logging.info(
        f"Emparejamientos completados: {len(emparejamientos)} encontrados"
        )
    return emparejamientos


def buscar_debitos_segun_cantidad(cantidad, tolerancia=0.05):
    _, debitos = obtener_datos_de_excel()
    combinacion = encontrar_combinacion_optima(debitos, cantidad, tolerancia=tolerancia)
    return combinacion
