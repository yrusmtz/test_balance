import sqlite3
from datetime import datetime, timedelta
import random
import logging
import time
import pandas as pd

logging.basicConfig(
    level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s'
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


# %%
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


df_transacciones = pd.read_excel('transacciones.xlsx')
df_transacciones
df_transacciones.groupby('TRAACR').count().sort_values(
    'TRAACR', ascending=False
    )
# Cambiar el nombre de las columnas
df_transacciones = df_transacciones.rename(
    columns={'TRAACR': 'id_transaccion', 'Db': 'debito', 'Cr': 'credito'}
    )

df_transacciones = df_transacciones[['id_transaccion', 'debito', 'credito']]

# id_transaccion debe ser texto
df_transacciones['id_transaccion'] = df_transacciones['id_transaccion'].astype(str)

df_transacciones


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


df_creditos, df_debitos = obtener_datos_de_excel()


def encontrar_combinacion_optima(
        debitos, credito, start=0, current_sum=0, current_comb=[],
        tolerancia=1.0
        ):
    logging.debug(
        f"Iniciando encontrar_combinacion_optima con credito={credito}, start={start}, current_sum={current_sum}, "
        f"tolerancia={tolerancia}"
        )
    # if current_sum == credito:
    if abs(current_sum - credito) <= tolerancia:
        logging.debug(f"Combinación encontrada: {current_comb}")
        return current_comb
    if current_sum > credito or start >= len(debitos):
        return None
    incluir = encontrar_combinacion_optima(
        debitos, credito, start + 1, current_sum + debitos[start][1],
                          current_comb + [debitos[start]]
        )
    if incluir is not None:
        return incluir
    excluir = encontrar_combinacion_optima(
        debitos, credito, start + 1, current_sum, current_comb
        )
    return excluir


def backtracking_combinacion_optima(
        debitos, credito, start=0, current_sum=0, current_comb=[],
        tolerancia=0.05
        ):
    if abs(current_sum - credito) <= tolerancia:
        return current_comb
    if current_sum > credito or start >= len(debitos):
        return None
    for i in range(start, len(debitos)):
        incluir = backtracking_combinacion_optima(
            debitos, credito, i + 1, current_sum + debitos[i][1],
                              current_comb + [debitos[i]]
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


def main():
    print("Emparejamiento de créditos y débitos")
    start_time = time.time()
    emparejamientos = emparejar_creditos_debitos()
    end_time = time.time()
    print(f"Tiempo de ejecución: {end_time - start_time} segundos")
    for emparejamiento in emparejamientos:
        print(emparejamiento)
    # agrupar las transacciones en un solo dataframe para guardarlas en un archivo Excel
    rows = []
    for group_id, (credito, debitos) in enumerate(emparejamientos, start=1):
        balance = 0
        # Agregar el crédito con su grupo
        balance += credito[1]
        rows.append((group_id, credito[0], 0, credito[1], balance))
        # Agregar cada débito con el mismo grupo
        for debito in debitos:
            balance -= debito[1]
            rows.append((group_id, debito[0], debito[1], 0, balance))
        rows.append((group_id, 'balance', 0, 0, balance))
    df_emparejamientos = pd.DataFrame(rows, columns=['group_id', 'id_transaccion', 'debito', 'credito', 'balance'])
    # guardar los resultados en un archivo Excel
    df_emparejamientos.to_excel('emparejamientos.xlsx', index=False)

    emparejamiento_backtracking = emparejar_creditos_debitos_backtracking()
    for emparejamiento in emparejamiento_backtracking:
        print(emparejamiento)
    rows = []
    for group_id, (credito, debitos) in enumerate(emparejamiento_backtracking, start=1):
        balance = 0
        # Agregar el crédito con su grupo
        balance += credito[1]
        rows.append((group_id, credito[0], 0, credito[1], balance))
        # Agregar cada débito con el mismo grupo
        for debito in debitos:
            balance -= debito[1]
            rows.append((group_id, debito[0], debito[1], 0, balance))
        rows.append((group_id, 'balance', 0, 0, balance))
    df_emparejamientos = pd.DataFrame(rows, columns=['group_id', 'id_transaccion', 'debito', 'credito', 'balance'])
    # guardar los resultados en un archivo Excel
    df_emparejamientos.to_excel('emparejamientos_backtracking.xlsx', index=False)


if __name__ == '__main__':
    # establecer el nivel de registro a INFO o DEBUG para ver los mensajes de depuración
    logging.getLogger().setLevel(logging.INFO)
    # tabla_de_prueba()
    main()
