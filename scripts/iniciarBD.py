# -*- coding: utf-8 -*-
"""
Created on Sat Sep 10 12:10:25 2022

@author: vi
"""

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from bs4 import BeautifulSoup
import re
from datetime import date, datetime as dt, timedelta 
import os
import pandas as pd

#!pip install wget        # luego copiar wget.exe en C:\Windows\System32
import subprocess

#!pip install tabula-py   # necesita que java este instalado
import tabula

import config
from config import ruta_temp
from sqlalchemy import create_engine

# diccionarion meses del año
meses = {'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04', 'mayo': '05', 'junio': '06', 
         'julio': '07', 'agosto': '08', 'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12' }

# web del ministerio de cultura y deporte
web = "https://www.culturaydeporte.gob.es"
# dirección de la página web del ministerio de cultura y deporte donde se recogen los datos de taquilla, número de espectadores y recaudación.
URL = web + "/cultura/areas/cine/datos/taquilla-espectadores.html"


### WEBSCRAPPING
def archivos_taquilla():
  """obtiene mediante webscrapping la lista de archivos disponibles en la página web del ministerio de cultura y deporte donde se recogen los datos de taquilla
  
  Returns: dataframe (periodicidad | fecha | semana | tipo | archivo | url)
  """
  # evitar mostrar avisos
  requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

  archivos  = []

  page = requests.get(URL, verify=False)
  soup = BeautifulSoup(page.content, "html.parser")

  #sección última información semanal
  tablas = soup.find_all("div", class_="info")
  # patron para buscar archivos con última información acumulada (tipo) (dia) (mes)
  regExp = r"^(Acumulado).* (\d{,2}) ([\w ]+)$"
  
  for t in tablas :
      enlace = t.find("a").text.strip()   
      url = web + t.find("a").attrs['href']     
      result = re.search(regExp, enlace)
      if not (result  is None):
        mes = result[3].split()[-1]
        fecha = date(dt.today().year, int(meses.get(mes)), int(result[2]))
        doc = {'periodicidad': "semanal",
               'fecha': fecha,
               'semana': str(fecha.isocalendar()[0]) + str(fecha.isocalendar()[1]).zfill(2),
               'tipo': result[1].lower(),
               'archivo': os.path.basename(url),
               'url': url
               }
        archivos.append(doc)
    
  # sección Histórico  ficheros anuales soup.find("h3", class_="subrayado").text
  tabla = soup.find("div", class_="cblq fondo")
  lista = tabla.find_all("li")
  #patrón para buscar archivos anuales
  regExp = r"^.* (\d{4}).*"

  for e in lista:
    enlace = e.find("a").text.strip()
    url = web + e.find("a").attrs['href']
    result = re.search(regExp, enlace)
    doc = {'periodicidad': "anual",
           'fecha': date(int(result[1]), 12, 31),
           'semana': result[1] + date(int(result[1]), 12, 28).strftime("%V"),
           'tipo': "acumulado",
           'archivo': os.path.basename(url),
           'url': url 
          }
    archivos.append(doc)

  #secciones columnas anuales ficheros semanales
  tablas = soup.find_all("div", class_="col")
  # patrón para buscar archivos semanales  (tipo): (dia1) - (dia) (mes)
  regExp = r"^(Top|Cine).*: (\d{,2}).* - (\d{,2}).* (\w+)$"
  # patrón para archivo semanal a caballo entre 2 años
  regExpURL =r".*(\d{4}).pdf$"
  
  for t in tablas :
    if len( t.get_text ( strip = True )) != 0:
      anho_col = t.find("h3", class_="subrayado").text.strip()[:4]  # el título de la columna es el año
      
      lista = t.find_all("li")  # todos los elemtos de listas
      for e in lista:
        enlace = e.find("a").text.strip()   
        url = web + e.find("a").attrs['href']
        # semana a caballo de 2 años
        if not (re.search(regExpURL, url)  is None): 
            anho = re.search(regExpURL, url)[1]
        else:
            anho = anho_col
            
        result = re.search(regExp, enlace)
        fecha = date(int(anho), int(meses.get(result[4])), int(result[3]))
        doc = {'periodicidad': "semanal", 
               'fecha': fecha,
               'semana': str(fecha.isocalendar()[0]) + str(fecha.isocalendar()[1]).zfill(2),
               'tipo': result[1].lower(),
               'archivo': os.path.basename(url),
               'url': url 
              }
        archivos.append(doc)

  return pd.DataFrame(archivos).sort_values(by = ['periodicidad', 'semana'], ascending = [True, True]).reset_index(drop = True )


### DESCARGAR FICHEROS
#https://www.scrapingbee.com/blog/python-wget/
def runcmd(cmd, verbose = False, log_file='', *args, **kwargs):
    """ Ejecuta comando cmd, cuando verbose es True y se le pasa el parametro log_file el resultado va a un fichero de log """
    process = subprocess.Popen(
        cmd,
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE,
        text = True,
        shell = True
    )
    std_out, std_err = process.communicate()
    if verbose:
      if log_file == '': #resultado por pantalla
        print(std_out.strip(), std_err)
      else:
        #resultado registrado en  fichero
        with open(log_file, 'a') as f:        
          f.write(f'{std_out.strip()}\n{std_err}')
    pass


def descargar_archivos(df):
  """
  descarga de la web una lista de archivos
  
  Parameters: df  dataframes con lista de urls de los archivos a descargar 
            (periodicidad | fecha | semana | tipo | archivo | url)
  """
  # nombre de fichero donde logar resultado de la descarga
  fichero_log = f'{dt.today().strftime("%Y%m%d")}_descarga.log'
  # ruta donde se descargan los ficheros
  ruta = ruta_temp  
  if df.empty:
    with open(fichero_log, 'a') as f:        
      f.write(f'{dt.now()} NO HAY NUEVOS FICHEROS')
  else:
    if not os.path.exists(ruta): os.makedirs(ruta)
    # descarga
    for dir_web in df['url']:
      fichero = os.path.basename(dir_web)
      runcmd(f"wget --no-check-certificate --output-document={ruta}{fichero} {dir_web}", verbose = True, log_file=fichero_log) 
    


### EXTRAER DATOS    
def corregir_formato(df, tipo):
  """
  ajusta la estructura de un dataframe al formato necesario dependiendo del tipo de archivo del que proviene
  
  Parameters: df,    dataframe con el dato "crudo" sin estructura bien definida
              tipo,  tipo de fichero
  Returns: df, dataframe con estructura
           tipo=acumulado -> df (RANK | TITULO | TIT_ORIGINAL | DISTRIBUIDORA | FECHA_ESTRENO | RECAUDACION | ESPECTADORES')
           tipo!=acumulado -> df (RANK | TITULO | TIT_ORIGINAL | DISTRIBUIDORA | SEM_ESTRENO | CINES | PANTALLAS | REC_SEM | REC_INCR% | ESP_SEM | ESP_INCR | RECAUDACION | ESPECTADORES')
  """   
  if tipo == 'acumulado':
    # renombrar columnas
    df.columns =  ['RANK','TITULO', 'TIT_ORIGINAL','DISTRIBUIDORA', 'FECHA_ESTRENO', 'RECAUDACION', 'ESPECTADORES']
    # eliminar filas cuya primera columna no empieza con dígito
    df.drop(df[df.RANK.apply(lambda x : re.match(r'^[\d]', str(x)) is None )].index, inplace=True) 

    # recolocar filas desplazadas
    recolocar = df[df[df.columns[3]].apply(lambda x : re.match(r'^\d{2}/\d{2}/\d{4}', str(x)) is not None )].index
    df.loc[recolocar, [ 'DISTRIBUIDORA', 'FECHA_ESTRENO', 'RECAUDACION', 'ESPECTADORES']] = df.loc[recolocar, ['TIT_ORIGINAL','DISTRIBUIDORA', 'FECHA_ESTRENO', 'RECAUDACION' ]].values

    # separar titulo de ranking
    if (df.RANK.apply(lambda x: x.isnumeric()).all() == False):
      df[['RANK','TITULO']] = df.RANK.str.split(' ', n=1, expand=True)
        
    df.TIT_ORIGINAL = df.TITULO
    df['FECHA_ESTRENO'] = pd.to_datetime(df['FECHA_ESTRENO'], format="%d/%m/%Y").dt.date
  else:
    # eliminar filas cuya primera columna no empieza con dígito
    df.drop(df[df[df.columns[0]].apply(lambda x: re.match(r'^[\d]', str(x)) is None )].index, inplace=True) 
    # eliminar columnas con todas las filas NaN  
    df.dropna(axis = 1, how='all', inplace = True)    

    if tipo == 'cine':
      # elimina columnas extras
      df.drop(axis=1, columns=df.columns[[8,9,12,13]], inplace=True) 
      # renombrar columnas
      df.columns = ['RANK','TITULO', 'DISTRIBUIDORA', 'SEM_ESTRENO', 'CINES', 'PANTALLAS', 'REC_SEM','REC_INCR%', 'ESP_SEM', 'ESP_INCR%', 'RECAUDACION', 'ESPECTADORES']
      # añadir columna
      df.insert(loc = 2, column = 'TIT_ORIGINAL', value =  df.TITULO ) 
    elif tipo == 'top':
      if len(df.columns) == 12:
        # renombrar columnas
        df.columns = ['RANK','TITULO', 'DISTRIBUIDORA', 'SEM_ESTRENO', 'CINES', 'PANTALLAS', 'REC_SEM','REC_INCR%', 'ESP_SEM', 'ESP_INCR%', 'RECAUDACION', 'ESPECTADORES']
        # añadir columna
        df.insert(loc = 2, column = 'TIT_ORIGINAL', value = '' )    
      elif   len(df.columns) == 13: 
        columnas=['RANK','TITULO', 'TIT_ORIGINAL','DISTRIBUIDORA', 'SEM_ESTRENO', 'CINES', 'PANTALLAS', 'REC_SEM','REC_INCR%', 'ESP_SEM', 'ESP_INCR%', 'RECAUDACION', 'ESPECTADORES']
        df.columns = columnas  

    df.REC_SEM = df.REC_SEM.str.translate(str.maketrans("","","€ .")).astype(int)
    df.ESP_SEM = df.ESP_SEM.astype(str).str.replace(".","", regex=False).astype(int)
    df[['REC_INCR%', 'ESP_INCR%']] = df[['REC_INCR%', 'ESP_INCR%']].apply( lambda x: x.str.replace("%","", regex=False)).fillna(0).astype(int)
    df[['SEM_ESTRENO', 'CINES', 'PANTALLAS']] = df[['SEM_ESTRENO', 'CINES', 'PANTALLAS']].fillna(1).astype(int)
    
  # convertir a minúsculas
  df[['TITULO','TIT_ORIGINAL', 'DISTRIBUIDORA']] = df[['TITULO','TIT_ORIGINAL', 'DISTRIBUIDORA']].apply( lambda x: x.str.lower())
  # asegurar un único espacio entre palabras
  df[['TITULO','TIT_ORIGINAL']] = df[['TITULO','TIT_ORIGINAL']].apply( lambda x: x.str.split(n=-1, expand=False).apply(" ".join), axis = 1)
  # convertir a valores numéricos
  df.RECAUDACION = df.RECAUDACION.str.translate(str.maketrans("","","€ .")).astype(int)
  df.ESPECTADORES = df.ESPECTADORES.astype(str).str.replace(".","", regex=False).astype(int)

  return df


def leer_archivo(archivo):
    """ devuelve un dataframe con contenido del archivo pdf
    
    Parameters: archivo, cadena con la ruta y el nombre del archivo a leer
    Returns: datos,  dataframe con los datos de la tabla del archivo
    """
    try:
      #leer tabla del fichero
      tabla = tabula.read_pdf(archivo, stream = True, guess = False, pages="all", multiple_tables=False, silent=True)
      datos = tabla[0]
    except pd.errors.ParserError :
      # algunos ficheros de datos acumulados no se leen como una unica tabla
      columnas=['col'+str(i) for i in range(7)]
      tablas = tabula.read_pdf(archivo, stream = True, guess = False, pages="all", multiple_tables=True)
      for t in tablas:
        if len(t.columns) == 6: 
          t.insert(loc = 2, column = 'col', value = '' )  
        t.rename(columns=dict(zip(t.columns, columnas)), inplace=True)
      datos= pd.concat(tablas).reset_index(drop =True)
    except OSError: pass 

    return datos


def obtener_dato(tipo, semana, log=[]):
  """
  devuelve el dato de un archivo estructurado según su tipo
  
  Parameters: tipo, (acumulado, top, cine) tipo de archivo del que se quiere extraer el dato 
              semana, fila de un dataframe con informacion de los archivos
  Returns: datos dataframe con estructura
           tipo=acumulado -> df (RANK | FECHA_INFO | SEMANA_INFO | TITULO | TIT_ORIGINAL | DISTRIBUIDORA | FECHA_ESTRENO | RECAUDACION | ESPECTADORES')
           tipo!=acumulado -> df (RANK | FECHA_INFO | SEMANA_INFO | TITULO | TIT_ORIGINAL | DISTRIBUIDORA | SEM_ESTRENO | CINES | PANTALLAS | REC_SEM | REC_INCR% | ESP_SEM | ESP_INCR | RECAUDACION | ESPECTADORES')
  """
  try:
    ruta = ruta_temp  
    if tipo =='cine': 
      fichero = semana.cine
    elif tipo == 'top':
      fichero = semana.top
    elif tipo == 'acumulado':
      fichero = semana.archivo
    
    #leer contenido del fichero
    datos = leer_archivo(ruta + fichero)
        
    # reorganizar dato
    corregir_formato(datos, tipo)

    # información adicional
    datos.insert(loc = 1, column = 'SEMANA_INFO', value =  semana.semana) 
    datos.insert(loc = 1, column = 'FECHA_INFO', value = semana.fecha   ) 

  except Exception as err:
    log.append((semana.semana, tipo, fichero, f'{type(err).__name__}: {err}'))
    return pd.DataFrame()
  else:
    return datos 


def obtener_dato_anual(df, log=[]):
  """
  devuelve el dato de una lista de archivos de tipo acumulado 
  
  Parameters: df, dataframe con los archivos de tipo acumulado
                  (periodicidad | fecha | semana | tipo | archivo | url)
  Returns: df_anual, dataframe con el dato de todos los archivos de tipo acumulado
              ( FECHA_INFO | SEMANA_INFO | TITULO | TIT_ORIGINAL | DISTRIBUIDORA | FECHA_ESTRENO | RECAUDACION | ESPECTADORES')  
  """
  df_anual = pd.DataFrame()

  for fila in df.itertuples():
    # extraer dato del archivo
    df_acu = obtener_dato(fila.tipo, fila, log)
    # añadir dato al resultado
    df_anual = pd.concat([df_anual, df_acu ] )

  df_anual.reset_index(drop = True, inplace = True)

  return df_anual[df_anual.columns[1:]]


def obtener_dato_semanal(df, log=[]):
  """
  devuelve el dato de una lista de archivos 
  
  Parameters: df, dataframe con los archivos semanales no acumulados
                  (semana | fecha | cine | top)
  Returns: df_sem, dataframe con el dato de todos los archivos semanales
            (FECHA_INFO | SEMANA_INFO | TITULO | TIT_ORIGINAL | DISTRIBUIDORA | SEM_ESTRENO | CINES | PANTALLAS | REC_SEM | REC_INCR% | ESP_SEM | ESP_INCR | RECAUDACION | ESPECTADORES')
  """    
  df_sem = pd.DataFrame()

  for semana in df.itertuples():
    # extraer dato de los archivos semanales
    df_cine = obtener_dato('cine', semana, log)
    df_top = obtener_dato('top', semana, log)  
    
    if not (df_top.empty or df_cine.empty) :
      # evitar repetición de peliculas españolas
      comun = df_top.merge(df_cine, how='inner', left_on = df_top.TITULO, right_on = df_cine.TITULO).RANK_x
      df_top.drop(axis=0,  index=df_top[df_top.RANK.isin (comun)].index  , inplace=True)   

      # evitar repetición para ficheros que se cargan uniendo titulo y titulo original
      comun = df_top.merge(df_cine, how='inner', left_on = df_top.TITULO, right_on = df_cine.TITULO +' '+ df_cine.TITULO).RANK_x
      df_top.drop(axis=0,  index=df_top[df_top.RANK.isin (comun)].index  , inplace=True)

    # unir datos
    df_sem = pd.concat([df_sem, df_top, df_cine]  )

  df_sem.reset_index(drop = True, inplace = True)

  return df_sem[df_sem.columns[1:]]


def lista_peliculas(lista_datos):
  """
  devuelve la lista de peliculas distintas de todos los archivos
  
  Parameters: lista_datos, lista de dataframes con datos acumulados y semanales
  Returns: pelis, dataframe (TITULO	| TIT_ORIGINAL | DISTRIBUIDORA | FECHA | TMDB_ID)
  """
  pelis = pd.DataFrame()
  for df in lista_datos:
    if 'FECHA_ESTRENO' in df.columns:
      columnas = ['TITULO', 'TIT_ORIGINAL','DISTRIBUIDORA', 'FECHA_ESTRENO']
    else:
      columnas = ['TITULO', 'TIT_ORIGINAL','DISTRIBUIDORA', 'FECHA_INFO', 'SEM_ESTRENO']
  
    pelis = pd.concat([pelis, df[columnas]])
  
  # si la película no tiene fecha de estreno poner la del viernes #SEM_ESTRENO anterior a la fecha de la información
  pelis.loc[pelis.FECHA_ESTRENO.isnull(), 'FECHA_ESTRENO'] = pelis.loc[pelis.FECHA_ESTRENO.isnull(),['FECHA_INFO','SEM_ESTRENO'] ].apply(lambda row: (row.FECHA_INFO - timedelta(days = 7*int(row.SEM_ESTRENO)-5)), axis=1)

  pelis.drop(axis=1, columns=['FECHA_INFO','SEM_ESTRENO'], inplace=True) 
  pelis.drop_duplicates(subset=['TITULO', 'TIT_ORIGINAL','DISTRIBUIDORA'], inplace=True)
  pelis.rename(columns={'FECHA_ESTRENO':'FECHA'}, inplace = True)
  pelis['TMDB_ID']=0
  pelis.reset_index(drop=True, inplace = True)

  return pelis


### CREAR BASE DE DATOS
def crear_BD():
  mysql = create_engine(f'mysql+pymysql://{config.db_user}:{config.db_pwd}@{config.db_host}:{config.db_port}')
  mysql.execute(f"CREATE DATABASE IF NOT EXISTS {config.db_name} " )
    
def conectar_bd():
  bd = create_engine(f'mysql+pymysql://{config.db_user}:{config.db_pwd}@{config.db_host}:{config.db_port}/{config.db_name}' )
  conexion = bd.connect()
  return conexion

def desconectar_bd(bd):
  bd.close()

def guardar_tabla(df, tabla, bd, tipo='append', id=False):
  """
  guarda el contenido de dataframe en una tabla en la base de dato que indica la conexión
  
  Parameters: df, DataFrame con los datos a ser guardados
              tabla, nombre de la tabla en la que guardar los datos
              bd, conexión a la base de datos
              tipo, 'append' añade datos a la tabla; 'replace' sobreescribe la tabla si existe y si no la crea; 'fail' error si la tabla existe
              id, True crea columna ID como clave primaria con valores automáticos
  """
  if (tipo == 'replace') and id:   
    df.index.name = 'id'
    df.reset_index(inplace = True)
    df.id = df.id+1
    df.to_sql(name=tabla, con=bd, if_exists=tipo, index=False)
    bd.execute(f"ALTER TABLE {tabla} MODIFY COLUMN id INT NOT NULL AUTO_INCREMENT PRIMARY KEY ;")
  else:
    df.to_sql(name=tabla, con=bd, if_exists=tipo, index=False)
    
def leer_tabla(tabla, filtro=''):
  """
  lee el contenido de una tabla en un dataframe
  
  Parameters: tabla, nombre de la tabla de la que obteber los datos
              filtro, cadena para construir la condicción de la query sql
  Returns: datos, dataframe con e contenido de la tabla
  """
  bd = conectar_bd()
  # condición
  if filtro : filtro = f' WHERE {filtro}'
  datos = pd.read_sql (f"select  * from {tabla} {filtro};", bd)
  desconectar_bd(bd)
  return datos


def borrar_datos(tabla, filtro=''):
  """
  borra datos de una tabla  
  
  Parameters: tabla, nombre de la tabla de la que borrar los datos
              filtro, cadena para construir la condicción de la  sentencia de borrado
  Returns: datos, dataframe con e contenido de la tabla
  """
  bd = conectar_bd()
  # condición
  if filtro : filtro = f' WHERE {filtro}'
  bd.execute(f"DELETE FROM {tabla} {filtro};")
  desconectar_bd(bd) 



if __name__ == '__main__':
  # identificar archivos webscraping
  df_archivos = archivos_taquilla()

  descargar_archivos(df_archivos)

  # extraer dato de archivos
  log=[] # inicializar lista de errores
  dato_anual = obtener_dato_anual(df_archivos[df_archivos['tipo']=="acumulado"], log)

  # lista de archivos semanales
  df_arch_sem = df_archivos[df_archivos['tipo']!="acumulado"].pivot_table(index=[ 'semana', 'fecha'], columns='tipo',
                                                                          values='archivo', aggfunc='first').reset_index()
  dato_semanal = obtener_dato_semanal(df_arch_sem, log)

  # añadir a la lista de archivos los errores durante procesado
  df_log = pd.DataFrame(log, columns = ['semana','tipo','archivo', 'error' ])
  archivos = pd.merge(df_archivos, df_log, how="left", on =['semana','tipo','archivo'])
  
  peliculas = lista_peliculas([dato_anual, dato_semanal ])

  # guardar datos en BBDD
  crear_BD()
  db=conectar_bd()
  
  guardar_tabla(archivos, 'taquilla_archivos',  db, 'replace')
  guardar_tabla(dato_anual, 'taquilla_anual', db, 'replace')
  guardar_tabla(dato_semanal, 'taquilla_semanal', db, 'replace')
  guardar_tabla(peliculas, 'peliculas', db, 'replace', True)
  
  desconectar_bd(db)
  

  
  
  
