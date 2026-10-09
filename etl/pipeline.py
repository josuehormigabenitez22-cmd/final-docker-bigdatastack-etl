# Archivo pipeline que limpia el CSV de ventas en crudo

# Importamos las librerías
import csv
import psycopg2
from datetime import datetime
import time
import os


# Definimos una función que comprueba que PostgreSQL esté funcionando
def esperar_postgres(max_intentos=10):
    """Espera a que PostgreSQL esté listo para inicializar el pipeline."""

    for intento in range(max_intentos):
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "postgres"),
                port=int(os.getenv("DB_PORT", "5432")),
                user=os.getenv("DB_USER", "etl_user"),
                password=os.getenv("DB_PASS", "etl_secreto"),
                database=os.getenv("DB_NAME", "ventas"),
                connect_timeout=5
            )

            conn.close()

            print(f"[OK] PostgreSQL lista (intento {intento + 1})")
            return True

        except psycopg2.OperationalError:
            print(f"... Esperando PostgreSQL ... (intento {intento + 1})")
            time.sleep(2)

    raise Exception(
        f"[ERROR] PostgreSQL no ha respondido después de "
        f"{max_intentos} intentos."
    )


def leer_csv(filepath):
    """Lee el CSV en crudo y devuelve una lista de diccionarios."""

    with open(filepath, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def limpiar_fila(fila):
    """Limpia y valida una fila. Devuelve None si no es válida."""

    identificador = fila.get("id", "desconocido")

    # Validar cantidad
    try:
        cantidad = int(fila.get("cantidad", ""))

        if cantidad <= 0:
            print(
                f"  [AVISO] Fila {identificador}: "
                f"cantidad inválida ({cantidad})"
            )
            return None

    except (ValueError, TypeError):
        print(
            f"  [AVISO] Fila {identificador}: "
            "cantidad vacía o no numérica"
        )
        return None

    # Validar cliente
    cliente = (fila.get("cliente") or "").strip() or "Desconocido"

    # Normalizar fecha
    fecha_raw = (fila.get("fecha") or "").strip()

    try:
        if "/" in fecha_raw:
            fecha = datetime.strptime(
                fecha_raw, "%d/%m/%Y"
            ).date()
        else:
            fecha = datetime.strptime(
                fecha_raw, "%Y-%m-%d"
            ).date()

    except ValueError:
        print(
            f"  [AVISO] Fila {identificador}: "
            f"fecha inválida ({fecha_raw})"
        )
        return None

    # Validar precio unitario
    try:
        precio = float(
            (fila.get("precio_unitario") or "").strip()
        )

        if precio <= 0:
            print(
                f"  [AVISO] Fila {identificador}: "
                f"precio inválido ({precio})"
            )
            return None

    except (ValueError, TypeError):
        print(
            f"  [AVISO] Fila {identificador}: "
            "precio vacío o no numérico"
        )
        return None

    # Validar producto y ciudad
    producto = (fila.get("producto") or "").strip()
    ciudad = (fila.get("ciudad") or "").strip()

    if not producto or not ciudad:
        print(
            f"  [AVISO] Fila {identificador}: "
            "producto o ciudad vacíos"
        )
        return None

    # Calcular el importe total
    total = round(cantidad * precio, 2)

    return {
        "producto": producto,
        "cantidad": cantidad,
        "precio_unitario": precio,
        "total": total,
        "fecha": fecha,
        "cliente": cliente,
        "ciudad": ciudad,
    }


def cargar_en_postgres(filas_limpias):
    """Inserta las filas limpias en PostgreSQL."""

    if not filas_limpias:
        print("[AVISO] No hay filas válidas para insertar.")
        return 0

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "etl_user"),
        password=os.getenv("DB_PASS", "etl_secreto"),
        database=os.getenv("DB_NAME", "ventas"),
        connect_timeout=5
    )

    try:
        with conn:
            with conn.cursor() as cursor:
                for fila in filas_limpias:
                    cursor.execute(
                        """
                        INSERT INTO ventas_limpias
                        (
                            producto,
                            cantidad,
                            precio_unitario,
                            total,
                            fecha,
                            cliente,
                            ciudad
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            fila["producto"],
                            fila["cantidad"],
                            fila["precio_unitario"],
                            fila["total"],
                            fila["fecha"],
                            fila["cliente"],
                            fila["ciudad"],
                        )
                    )

        return len(filas_limpias)

    finally:
        conn.close()


if __name__ == "__main__":
    print("=" * 50)
    print("Pipeline ETL iniciado")
    print("=" * 50)

    # Comprobar la conexión con PostgreSQL
    esperar_postgres()

    # EXTRACT
    print("\nExtrayendo datos del CSV...")

    datos_crudos = leer_csv("/app/data/ventas_raw.csv")

    print(f"   Filas leídas: {len(datos_crudos)}")

    # TRANSFORM
    print("\nLimpiando y transformando...")

    datos_limpios = []

    for fila in datos_crudos:
        limpia = limpiar_fila(fila)

        if limpia is not None:
            datos_limpios.append(limpia)

    print(
        f"   Filas válidas: "
        f"{len(datos_limpios)}/{len(datos_crudos)}"
    )

    # LOAD
    print("\nCargando en PostgreSQL...")

    insertadas = cargar_en_postgres(datos_limpios)

    print(f"   Filas insertadas: {insertadas}")

    print("\n[OK] Pipeline completado exitosamente!")
    print("=" * 50)

