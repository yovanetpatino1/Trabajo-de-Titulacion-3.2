import pandas as pd
from pymongo import MongoClient
import glob
import os
import numpy as np


def categorizar_desnutricion(valor: str) -> str:
    if "SEVERA" in valor:
        return "SEVERA"
    elif "MODERADA" in valor:
        return "MODERADA"
    elif "LEVE" in valor:
        return "LEVE"
    else:
        return "NO ESPECIFICADA"

#def cargar_y_procesar_datos(ruta_excel):
    #columnas_relevantes = [

    #columnas_relevantes = [
    #"PCTE_IDE", "PCTE_SEXO", "PCTE_FEC_NAC", "PCTE_ANIOS_EN_MESES", "PCTE_PESO", "PCTE_TALLA",
    #"PCTE_ULT_IMC", "PCTE_CAT_PESO_EDAD_Z", "PCTE_CAT_IMC_EDAD_Z", "PCTE_CAT_PESO_LONGTALLA_Z",
    #"ATEMED_CIE10", "ATEMED_DES_CIE10", "NIVEL_DESCRIPCION",
    #"ENT_DES_PROV", "ENT_COD_PROV", "ENT_DES_CANT", "ENT_COD_CANT",
    #"ENT_DES_PARR", "ENT_COD_PARR", "ENT_DES_TIP_PARR"
    #]


    #df = pd.read_excel(ruta_excel)

    #faltantes = [col for col in columnas_relevantes if col not in df.columns]
    #if faltantes:
    #    raise ValueError(f"Faltan columnas: {faltantes}")

    #df = df[columnas_relevantes]

    # Normaliza y categoriza
    #df["NIVEL_CATEGORIZADO"] = df["ATEMED_DES_CIE10"].apply(lambda x: categorizar_desnutricion(str(x).upper()))

    # Solo conservar filas con niveles reconocidos (sin NO ESPECIFICADA si deseas)
    #df = df[df["NIVEL_CATEGORIZADO"] != "NO ESPECIFICADA"]

    #df.dropna(subset=["PCTE_IDE", "PCTE_PESO", "PCTE_TALLA"], inplace=True)

    #return df

import pandas as pd
import numpy as np

def cargar_y_procesar_datos(ruta_excel):
    columnas_relevantes = [
        # Niño
        "PCTE_IDE", "PCTE_SEXO", "PCTE_FEC_NAC", "PCTE_ANIOS_EN_MESES",
        "PCTE_PESO", "PCTE_TALLA", "PCTE_ULT_IMC_EDAD_Z",
        "PCTE_CAT_PESO_EDAD_Z", "PCTE_CAT_IMC_EDAD_Z", "PCTE_CAT_PESO_LONGTALLA_Z",
        # Diagnóstico
        "ATEMED_CIE10", "ATEMED_DES_CIE10",
        # Ubicación
        "ENT_COD_PROV", "ENT_DES_PROV",
        "ENT_COD_CANT", "ENT_DES_CANT",
        "ENT_COD_PARR", "ENT_DES_PARR", "ENT_DES_TIP_PARR",
        # Centro de Salud (los nombres exactos de tu Excel)
        "ENT_ID",           # si quieres el código interno
        "ENT_RUC",
        "ENT_NOM",
        "ENT_SIM_TIP_EST",  # <-- aquí estaba el error
        "ENT_DES_TIP_EST"
    ]

    # 1) Carga
    df = pd.read_excel(ruta_excel)
    df.columns = df.columns.str.strip()

    falt = [c for c in columnas_relevantes if c not in df.columns]
    if falt:
        raise ValueError(f"Faltan columnas en {ruta_excel}: {falt}")

    df = df[columnas_relevantes].copy()

    # 1) ENT_ID como entero, ENT_RUC como string limpio
    df["ENT_ID"]  = pd.to_numeric(df["ENT_ID"], errors="coerce").astype("Int64")
    df["ENT_RUC"] = df["ENT_RUC"].astype(str).str.replace(r"\.0+$","",regex=True)

    # 2) Quitar columnas vacías
    df.dropna(axis=1, how="all", inplace=True)

    # 3) Eliminar duplicados por niño+fecha
    df.drop_duplicates(subset=["PCTE_IDE","PCTE_FEC_NAC"], keep="last", inplace=True)

    # 4) Tipos: fecha y numéricos
    df["PCTE_FEC_NAC"] = pd.to_datetime(df["PCTE_FEC_NAC"], errors="coerce")
    for col in ["PCTE_ANIOS_EN_MESES","PCTE_PESO","PCTE_TALLA","PCTE_ULT_IMC_EDAD_Z"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 5) Intervalo de edad
    bins   = [-np.inf,12,24,36,60,np.inf]
    labels = ["0-12 meses","13-24 meses","25-36 meses","37-60 meses","Mayor a 60 meses"]
    df["INTERVALO_EDAD"] = pd.cut(df["PCTE_ANIOS_EN_MESES"], bins=bins, labels=labels)

    # 6) Limpieza de texto
    texto_cols = [
      "ATEMED_CIE10","ATEMED_DES_CIE10",
      "ENT_DES_PROV","ENT_DES_CANT","ENT_DES_PARR","ENT_DES_TIP_PARR",
      "ENT_NOM","ENT_SIM_TIP_EST","ENT_DES_TIP_EST"
    ]
    for col in texto_cols:
        df[col] = (df[col]
            .astype(str)
            .str.strip()
            .str.upper()
            .replace("NAN","")
            .str.replace(r"[^\w\s]","",regex=True)
        )

    # 7) Categorías faltantes
    for col in ["PCTE_CAT_PESO_EDAD_Z","PCTE_CAT_IMC_EDAD_Z","PCTE_CAT_PESO_LONGTALLA_Z"]:
        df[col] = df[col].fillna("DESCONOCIDO")

    # 8) Filtrar filas sin claves y sin datos esenciales
    df = df[df["PCTE_IDE"].notnull() & df["PCTE_FEC_NAC"].notnull()]
    df = df[df["PCTE_PESO"].notnull() & df["PCTE_TALLA"].notnull()]

    # 9) Categorizar desnutrición y filtrar “NO ESPECIFICADA”
    df["NIVEL_CATEGORIZADO"] = df["ATEMED_DES_CIE10"] \
        .apply(lambda x: categorizar_desnutricion(str(x).upper()))
    df = df[df["NIVEL_CATEGORIZADO"]!="NO ESPECIFICADA"]

    # 10) Rellenar nulos numéricos con mediana
    for col in ["PCTE_ANIOS_EN_MESES","PCTE_PESO","PCTE_TALLA","PCTE_ULT_IMC_EDAD_Z"]:
        med = df[col].median()
        df[col] = df[col].fillna(med)

    return df



#def guardar_en_mongo(df):
#    client = MongoClient("mongodb://localhost:27017/")
#    db = client["control_desnutricion"]
#    collection = db["casos"]

#    insertados, actualizados = 0, 0

#    for _, row in df.iterrows():
#        row_dict = row.to_dict()

        # Clave compuesta para identificar un registro único
#        filtro = {
#            "PCTE_IDE": row_dict["PCTE_IDE"],
#            "ATEMED_DES_CIE10": row_dict["ATEMED_DES_CIE10"],
#            "PCTE_FEC_NAC": row_dict["PCTE_FEC_NAC"]
#        }

#        resultado = collection.update_one(filtro, {"$set": row_dict}, upsert=True)

#        if resultado.matched_count:
#            actualizados += 1
#        else:
#            insertados += 1

#    print(f"✅ Insertados: {insertados}, 🔁 Actualizados: {actualizados}")



def guardar_en_mongo(df):
    client      = MongoClient("mongodb://localhost:27017/")
    db          = client["control_desnutricion"]

    col_ninos    = db["ninos"]
    col_prov     = db["provincias"]
    col_cant     = db["cantones"]
    col_parr     = db["parroquias"]
    col_centros  = db["centros_salud"]
    col_int      = db["intervalos"]
    col_niv      = db["niveles"]
    col_med      = db["mediciones"]

    insertados, actualizados = 0, 0

    # A) Upsert de TODOS los centros únicos, usando ENT_ID como clave
    centros = ( df[["ENT_ID","ENT_RUC","ENT_NOM","ENT_SIM_TIP_EST","ENT_DES_TIP_EST","ENT_COD_PARR"]]
               .drop_duplicates(subset=["ENT_ID"])
               .dropna(subset=["ENT_ID"]) )
    for _, c in centros.iterrows():
        col_centros.update_one(
            {"_id": int(c["ENT_ID"])} ,
            {"$set": {
                "ruc":               c["ENT_RUC"],
                "nombre":            c["ENT_NOM"],
                "simbolo_tipo":      c["ENT_SIM_TIP_EST"],
                "descripcion_tipo":  c["ENT_DES_TIP_EST"],
                "parroquia_id":      int(c["ENT_COD_PARR"])
            }},
            upsert=True
        )

    # B) Upsert resto de dimensiones y mediciones
    for _, row in df.iterrows():
        # Niño
        # Asignar idNivel y idIntervalo
        nivel_id = f"00{row['NIVEL_CATEGORIZADO'][:3].upper()}"
        intervalo_id = f"00{row['INTERVALO_EDAD'][:3].upper()}"

        # Asegurar que nino_id sea string
        nino_id = str(row["PCTE_IDE"])

        col_ninos.update_one(
            {"_id": nino_id},  # Aseguramos que nino_id es un string
            {"$set": {
                "fecha_nac": row["PCTE_FEC_NAC"],
                "sexo": row["PCTE_SEXO"],
                "idnivel": nivel_id,  # Agregar el id de nivel
                "idintervalo": intervalo_id  # Agregar el id de intervalo
            }},
            upsert=True
        )

        # Provincia
        col_prov.update_one(
            {"_id": row["ENT_COD_PROV"]},
            {"$set": {"nombre": row["ENT_DES_PROV"]}},
            upsert=True
        )

        # Cantón
        col_cant.update_one(
            {"_id": row["ENT_COD_CANT"]},
            {"$set": {"nombre": row["ENT_DES_CANT"], "provincia_id": row["ENT_COD_PROV"]}},
            upsert=True
        )

        # Parroquia
        col_parr.update_one(
            {"_id": row["ENT_COD_PARR"]},
            {"$set": {"nombre": row["ENT_DES_PARR"], "tipo": row["ENT_DES_TIP_PARR"], "canton_id": row["ENT_COD_CANT"]}},
            upsert=True
        )

        # Intervalo
        col_int.update_one(
            {"_id": intervalo_id},  # Usando el id del intervalo
            {"$set": {"descripcion": row["INTERVALO_EDAD"]}},
            upsert=True
        )

        # Nivel
        col_niv.update_one(
            {"_id": nivel_id},  # Usando el id del nivel
            {"$set": {"descripcion": row["NIVEL_CATEGORIZADO"]}},
            upsert=True
        )

        # Medición
        medicion = {
            "nino_id":        nino_id,  # Aseguramos que nino_id es un string
            "parroquia_id":   row["ENT_COD_PARR"],
            "centro_id":      int(row["ENT_ID"]),
            "diagnostico_id": row["ATEMED_CIE10"],
            "fecha":          row["PCTE_FEC_NAC"],
            "edad_meses":     row["PCTE_ANIOS_EN_MESES"],
            "peso":           row["PCTE_PESO"],
            "talla":          row["PCTE_TALLA"],
            "imc_z":          row["PCTE_ULT_IMC_EDAD_Z"]
        }
        filtro = {
            "nino_id":        medicion["nino_id"],
            "diagnostico_id": medicion["diagnostico_id"],
            "fecha":          medicion["fecha"]
        }
        res = col_med.update_one(filtro, {"$set": medicion}, upsert=True)
        if res.matched_count:
            actualizados += 1
        else:
            insertados += 1

    print(f"✅ Mediciones insertadas: {insertados}, 🔁 actualizadas: {actualizados}")





def main():
    # Ruta a la carpeta donde están tus .xlsx
    carpeta = "../1_ingesta"
    patron = os.path.join(carpeta, "*.xlsx")

    # Encuentra todos los .xlsx
    archivos = glob.glob(patron)
    print(f"Voy a procesar {len(archivos)} archivos .xlsx")

    for ruta in archivos:
        nombre = os.path.basename(ruta)
        print(f"→ Procesando {nombre} ...")
        try:
            df = cargar_y_procesar_datos(ruta)
            guardar_en_mongo(df)
            print(f"   ✔ {nombre} procesado y guardado.")
        except Exception as e:
            print(f"   ✘ Error al procesar {nombre}: {e}")

if __name__ == "__main__":
    #df = cargar_y_procesar_datos("../data/desnutricionExcel.xlsx")
    #guardar_en_mongo(df)
    main()
