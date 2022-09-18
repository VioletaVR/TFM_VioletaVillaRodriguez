# -*- coding: utf-8 -*-
"""
Created on Sun Sep 18 12:47:00 2022

@author: vi
"""

import iniciarBD as mi, informacionAdicional as inf
from iniciarBD import meses, web, URL

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from bs4 import BeautifulSoup
import re
from datetime import date, datetime as dt, timedelta 
import pandas as pd
import os


def Domingo(anho, mes, dia):
  """
  si la fecha dada no es domingo devuelve la fecha del siguiente domingo a la fecha dada
  
  Parameters: anho, mes, dia, integer para componer la fecha
  Returns:  fecha, date
  """
  fecha = date(anho, mes, dia)
  fecha = ( fecha + timedelta(days = 6-fecha.weekday())) if (fecha.weekday() != 6) else fecha
  
  return fecha


def nueva_info (semana ):
  """
  comprueba si ya hay archivos procesados para una semana dada
  
  Parameters: semana, cadena formato yyyyww
  Returns: True si no hay archivos en la base de datos para esa semana 
  """
  archivos = mi.leer_tabla('taquilla_archivos', f'semana = {semana} ;')

  return archivos.empty


def ultimos_archivos_taquilla():
  """
  obtiene mediante webscrapping la lista de los últimos archivos disponibles en la página web del ministerio de cultura y deporte donde se recogen los datos de taquilla
  
  Returns: dataframe (periodicidad | fecha | semana | tipo | archivo | url)
  """    
  # evitar mostrar avisos
  requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

  archivos  = []

  page = requests.get(URL, verify=False)
  soup = BeautifulSoup(page.content, "html.parser")

  #sección última información semanal
  tablas = soup.find_all("div", class_="info")
  # patron para buscar archivos con última información  (tipo) (dia) (mes)
  regExp = r"^(Top|Cine|Acumulado).* (\d{,2}) ([\w ]+)$"
  
  for t in tablas :
      referencia = t.find("a").text.strip()   
      url = web + t.find("a").attrs['href']     
      result = re.search(regExp, referencia)
      mes = result[3].split()[-1]
      fecha = Domingo(dt.today().year, int(meses.get(mes)), int(result[2]) )
      semana = str(fecha.isocalendar()[0]) + str(fecha.isocalendar()[1]).zfill(2) 
      if nueva_info(semana):     
        doc = {'periodicidad': "semanal",
               'fecha': fecha,
               'semana': semana,
               'tipo': result[1].lower(),
               'archivo': os.path.basename(url),
               'url': url
               }
        archivos.append(doc)

  # sección Histórico  
  tabla = soup.find("div", class_="cblq fondo")
  #patron para buscar archivos anuales
  regExp = r"^.* (\d{4}).*"
  # solo el primer archivo de la lista
  nombre = tabla.find("a").text.strip()
  url = web + tabla.find("a").attrs['href']
  result = re.search(regExp, nombre)
  semana = result[1] + date(int(result[1]), 12, 28).strftime("%V")
  # si hay nuevo fichero anual hay que procesarlo
  if nueva_info(semana): 
    doc = {'periodicidad': "anual",
           'fecha': date(int(result[1]), 12, 31),
           'semana': semana,
           'tipo': "acumulado",
           'archivo': os.path.basename(url),
           'url': url        
           }
    archivos.append(doc)

  if len(archivos)==0:
    df_archivos = pd.DataFrame(columns=['periodicidad','fecha','semana','tipo','archivo','url'])
  else:
    df_archivos = pd.DataFrame(archivos).sort_values(by = [  'periodicidad', 'semana'], ascending = [True, True])
    
  return df_archivos


def siguiente_peli ():
  """
  Returns: int, el siguiente número al último identificador de película 
  """
  db = mi.conectar_bd()
  peli = pd.read_sql ("select MAX(id) as id from peliculas;", db)
  mi.desconectar_bd(db)

  return peli.id.iat[0]+1


def nuevas_peliculas(da, ds):
  """
  Parameters: da, dataframe con dato acumulado semanal
              ds, dataframe con dato semanal 
  Returns: pelis, datframe con películas estrenadas la última semana
  """
  pelis = ds[ds.SEM_ESTRENO==1][['TITULO', 'TIT_ORIGINAL','DISTRIBUIDORA','FECHA_INFO']]
  pelis = pd.merge(pelis, da[["TITULO", "TIT_ORIGINAL", "DISTRIBUIDORA", "FECHA_ESTRENO"]], how="left", on =["TITULO", "TIT_ORIGINAL", "DISTRIBUIDORA"])
  # si la película no tiene fecha de estreno poner la del viernes anterior a la fecha de la información
  pelis.loc[pelis.FECHA_ESTRENO.isnull(), 'FECHA_ESTRENO'] = pelis.loc[pelis.FECHA_ESTRENO.isnull(),['FECHA_INFO'] ].apply(lambda row: (row.FECHA_INFO - timedelta(days = 2)), axis=1)
  pelis.drop(axis=1, columns='FECHA_INFO', inplace=True) 
  pelis['FECHA_ESTRENO'] = pd.to_datetime(pelis.FECHA_ESTRENO).dt.date
  pelis['TMDB_ID']=0
  pelis.index=list(range(siguiente_peli(),siguiente_peli()+len(pelis)))
  pelis.reset_index(inplace=True)
  pelis.rename (columns={'index':'id', 'FECHA_ESTRENO':'FECHA'}, inplace=True)

  return pelis 


def ultimos_datos(archivos):
  """
  procesa los últimos archivos publicados
  
  Parameters: archivos, dataframe con la lista de archivos
  """    
  log=[] # inicializar lista de errores

  # extraer dato acumulado
  dato_anual = mi.obtener_dato_anual(archivos[archivos['tipo']=="acumulado"], log)

  df_arch_sem = archivos[archivos['tipo']!="acumulado"].pivot_table(index=['semana', 'fecha'], columns='tipo',
                 values='archivo', aggfunc='first').reset_index()
  #extraer dato semanal
  dato_semanal = mi.obtener_dato_semanal(df_arch_sem, log)

  df_log = pd.DataFrame(log, columns = ['semana','tipo','archivo', 'error' ])
  archivos = pd.merge(archivos, df_log, how="left", on =["semana", "tipo", "archivo"])
  
  peliculas = nuevas_peliculas(dato_anual, dato_semanal)
  
  # obtenre identificador de TMDB
  inf.buscar_TMDB(peliculas)

  # guardar datos 
  db = mi.conectar_bd()
  
  mi.guardar_tabla(archivos, 'taquilla_archivos',  db, 'append')  
  mi.guardar_tabla(dato_anual, 'taquilla_anual', db, 'append')
  mi.guardar_tabla(dato_semanal, 'taquilla_semanal', db, 'append')
  mi.guardar_tabla(peliculas, 'peliculas', db, 'append' )
  
  mi.desconectar_bd(db)  
  
  # borrar datos acumulados de la semana anterior
  mi.borrar_datos('taquilla_anual', filtro=f'semana_info = {int(archivos.semana.iat[0])-1}')

  # extraer datos de TMDB para las nuevas películas
  inf.obtener_detalles(peliculas, 'append')
  
"""
  ## devolucion para poder visualizar un resultdo en notebook <<<<<<<<<<<<<
  #return peliculas 
"""
  
  
if __name__ == '__main__':
  # identificar archivos webscraping
  nuevos_archivos = ultimos_archivos_taquilla()
  
  mi.descargar_archivos(nuevos_archivos) 
  
  # procesar archivos
  ultimos_datos(nuevos_archivos) if not nuevos_archivos.empty else None
"""  
  #### devolucion para poder ver resultado en notebook <<<<<<<<<<<<<
  #peliculas = ultimos_datos(nuevos_archivos) if not nuevos_archivos.empty else None  
"""
  
  