# -*- coding: utf-8 -*-
"""
Created on Sat Sep 10 16:50:50 2022

@author: vi
"""

import config
import iniciarBD as mi
from datetime import datetime as dt, timedelta 
import requests
import pandas as pd
from pandas import isnull

API_KEY = config.api_key


### BUSCAR PELICULA CON API DE TMDB    
#https://developers.themoviedb.org/3/search/search-movies

def afinar_busqueda(titulo, fecha, lista, afina = 0):
  """
  funcion recursiva que para encontrar única coincidencia de una película en una lista de posibles 
  
  Parameters: titulo, cadena con nombre de pelicula
              fecha, fecha de estreno 
              lista, lista de resultados posibles de la búsqueda
              afina, contador de llamadas
  Returns: id, identificador de la pelicula en TMDB, -1 si no se encuentra
           msg, cadena con la lista de posibles peliculas, cadena vacia si encuentra una única
  """
  id =[p.get('id') for p in lista]
  #print(f'afina= {afina} / titulo= {titulo} / fecha= {fecha} / {id}')
    
  if afina == 0:
    # coincidencia por titulo exacto
    nueva_lista = [p  for p in lista if ((p.get('title').lower()==titulo) | (p.get('original_title').lower()==titulo)) ] 
  if afina == 1:
    # coincidencia por fecha de estreno
    nueva_lista = [p for p in lista if (p.get('release_date') == fecha.strftime("%Y-%m-%d")  ) ]
  if afina == 2:
    # coincidencia por fecha de estreno entre semana anterior y posterior
    sem_prev = (fecha - timedelta(weeks = 1)).strftime("%Y-%m-%d")
    sem_post = (fecha + timedelta(weeks = 1)).strftime("%Y-%m-%d")
    nueva_lista = [p for p in lista if (sem_prev< p.get('release_date') <sem_post  ) ]

  msg=""
  if len(nueva_lista)==1:
    id = [p.get('id') for p in nueva_lista]
  elif (len(nueva_lista)!=1) & (afina ==2):  
    msg= f'/ ({len(lista)}) / {id}'
    id = -1  
  elif (len(nueva_lista)==0)  :
    id, msg = afinar_busqueda(titulo, fecha, lista, afina+1)
  elif (len(nueva_lista)>1)  :
    id, msg  = afinar_busqueda(titulo, fecha, nueva_lista, afina+1)
    
  return id, msg


def buscar_peli(peli, busqueda = 0, tit_pal=[]):
  """
  busca el titulo de un apelícula en The Movie Database
  
  Parameters: peli,
              busqueda, contador de llamada
              tit_pal, lista de palabras 
  Retruns: id, lista de identificadores
           msg, cadena vacia si hay coincidencia, mensage con resultado de la búsqueda 
  """     
  fecha = peli.FECHA

  max_b = 3
  if busqueda == 0:
    titulo = peli.TITULO    
    titulo= titulo.translate(str.maketrans("","","-,:"))
    tit_pal = titulo.split()
    #colocar artículo
    if tit_pal[-1] in ['el', 'la', 'los', 'las', 'os']:
      titulo = tit_pal[-1] + " " + " ".join(tit_pal [:-1])
      tit_pal = titulo.split()
    busca = "+".join(tit_pal) + f"&year={fecha.year}"   
  elif busqueda == 1:
    titulo = " ".join(tit_pal ) 
    busca = "+".join(tit_pal ) + f"&year={fecha.year-1}" 
  elif busqueda == 2:
    if (peli.TIT_ORIGINAL ==''):
      titulo = " ".join(tit_pal[:len(tit_pal)//2] ) 
      busca = "+".join(tit_pal[:len(tit_pal)//2])  + f"&year={fecha.year}"        
    else:
      busca = ""     
  elif busqueda == 3:  
    if (peli.TIT_ORIGINAL !='') and (peli.TIT_ORIGINAL != peli.TITULO ):
      titulo = peli.TIT_ORIGINAL 
      titulo= titulo.translate(str.maketrans("","","-,.:"))
      tit_pal = titulo.split()
      if tit_pal[-1] in ['the']:
        titulo = tit_pal[-1] + " " + " ".join(tit_pal [:-1])
        tit_pal = titulo.split() 
      busca = "+".join(tit_pal) + f"&year={fecha.year}"  
    else: 
      titulo = "+".join(tit_pal[1:])
      busca = "+".join(tit_pal[1:]) + f"&year={fecha.year}" 

  tot_res = 0
  url=""
  if busca != "":
    url=f'https://api.themoviedb.org/3/search/movie?api_key={API_KEY}&query={busca}&language=es-ES&page=1&include_adult=false'
    r = requests.get(url)
    resultados = r.json().get('results')
    if not (r.json().get('total_results') is None): tot_res = r.json().get('total_results')  

  try:
      id = [0]
      msg = ""     
#       print(f'BUSQUEDA= {busqueda} / tot_res= {tot_res} / busca= {busca} / {url}')
      info = f'{peli.id} / {peli.TITULO} / {peli.FECHA} / \nbusqueda= {busqueda} / tot_res= {tot_res} / busca= {busca} / {url}'
      if (tot_res == 0) & (busqueda == max_b):
        msg = f'NO ENCONTRADO / {info}'
      elif (tot_res == 0) & (busqueda < max_b):
        #seguir buscando si el titulo tiene más de una palabra
        if not ((len(tit_pal)==1) & (busqueda ==1)):
          id, msg = buscar_peli(peli, busqueda+1, tit_pal)
        else:
          msg = f'NO ENCONTRADO / {info}'
      elif (tot_res == 1):
        # coincidencia
        id = [rp.get('id') for rp in resultados]
        msg = f'COINCIDENCIA / {info}'
      elif (tot_res > 1):
        # varios resultados posibles
        id, msg = afinar_busqueda(titulo, fecha, resultados)
        if msg!='':
          msg = f'VARIOS / {info}    {msg}'
        else:
          msg = f'AFINADO / {info}'
                   
  except Exception as err:
    msg = f'ERROR en buscar_peli - {type(err).__name__}: {err}\n     {msg}'

  return id, msg


def buscar_TMDB(peliculas):
  """
  actualiza la lista de peliculas añadiendo el identificador de TMDB
  
  Parameters: peliculas, dataframe con la lista de peliculas a buscar
  """
  busqueda_log = f'{dt.today().strftime("%Y%m%d")}_busqueda.log'
  
  with open(busqueda_log, 'a', encoding="utf-8") as f:             
    for peli in peliculas[peliculas.TMDB_ID==0].itertuples(): 
      try:
        id , msg = buscar_peli(peli)
        peliculas.loc[peli.Index, 'TMDB_ID']= id
      except Exception as err:
        msg = f'ERROR - {type(err).__name__}: {err}\n     {peli.id}, {peli.TITULO}, {peli.FECHA}'
      finally:      
        if msg != '': 
          #print(msg) 
          f.write(f'{msg}\n')         
          

def guardar_TMDB_ID(peliculas):
  """
  actualiza en la base de datos la tabla peliculas añadiendo los identificadores de TMDB
  
  Parameters: peliculas, dataframe con las peliculas actualizadas
  """
  db = mi.conectar_bd()
  
  # guardar tabla temporal
  mi.guardar_tabla(peliculas, 'temp_upd', db, 'replace', False)
  # actualizar tabla con información temporal
  sql = "UPDATE peliculas p INNER JOIN temp_upd U ON p.id = U.id SET p.TMDB_ID = U.TMDB_ID ;"
  db.execute(sql)
  # eliminar tabla temporal
  sql = "DROP TABLE temp_upd;"
  db.execute(sql)
  
  mi.desconectar_bd(db)
          

### EXTRAER DETALLES
#https://developers.themoviedb.org/3/movies/get-movie-details  (GET /movie/{movie_id} ) append_to_response:
#https://developers.themoviedb.org/3/movies/get-movie-credits  (GET /movie/{movie_id}/credits).   
#https://developers.themoviedb.org/3/movies/get-movie-keywords (GET /movie/{movie_id}/keywords).  

#https://developers.themoviedb.org/3/genres/get-movie-list     (GET /genre/movie/list).  
#https://developers.themoviedb.org/3/people/get-person-details (GET /person/{person_id}). 

def expandir_caracteristica(df, cols):
  """
  crea un nuevo dataframe a partir de una columna de tipo lista de diccionarios,
  añadiendo una fila por cada diccionario y luego creando una columna por cada clave
  
  Parameters: df, dataframe con el resultado de las peticiones
              cols, lista de 2 columnas del df 
                      la primera tiene que ser TMDB_ID y la segunda una columna de tipo lista de diccionarios
  """
  # converitr TMDB_ID en indice
  expansion = df[cols].set_index(cols[0]) 
  # una fila por diccionario y reconvertir TMDB_ID en columna
  expansion = pd.DataFrame(expansion[cols[1]].explode()).reset_index() 
  # convertir nulos en diccionarios vacios 
  expansion = expansion.applymap(lambda x: {} if isnull(x) else x)
  # convertir claves de los diccionarios en columnas y eliminar columna original
  expansion = expansion.join(pd.DataFrame(expansion.pop(cols[1]).values.tolist()))
  
  return expansion 


def extraer_generos(df, guardar='replace'):
  """
  guarda una tabla cruzada de generos por película y otra con todos los generos que proporciona la API de TMDB
  
  Parameters: df, dataframe con el resultado de las peticiones
              guardar, opcion para guardar la tabla (replace, append, fail)
  """  
  with open(detalles_log, 'a', encoding="utf-8") as f:   
    f.write(f'{dt.now()} inicio extraer géneros \n') 
    
  # columna genres de tipo lista de diccionarios [{id: , name:}]
  peli_generos = expandir_caracteristica(df, ['TMDB_ID','genres'])
  peli_generos.drop_duplicates(inplace=True)
  peli_generos = pd.crosstab(peli_generos.TMDB_ID, peli_generos.name)
  peli_generos = peli_generos.fillna(0).astype(int)
  peli_generos.reset_index(inplace=True)

  db = mi.conectar_bd()
  mi.guardar_tabla(peli_generos, 'peli_generos', db, guardar, False)

  # guardar lista de todos los generos de películas  
  if guardar=='replace': 
    url=f'https://api.themoviedb.org/3/genre/movie/list?api_key={API_KEY}'
    r = requests.get(url)
    generos = pd.DataFrame.from_dict(list(r.json()['genres']))
    mi.guardar_tabla(generos, 'generos', db, guardar, False)
  mi.desconectar_bd(db)  

  # eliminar columna extraida
  df.drop(axis=1, columns='genres', inplace=True)  
  

def obtener_personas(lista, guardar='replace'):
  """
  dada una lista de identificadores de personas de TMDB obtine sus detalles y los guarda en la tabla personas
  
  Parameters: lista, lista de identificadores
              guardar, opcion para guardar la tabla (replace, append, fail)      
  """   
  if guardar == 'append':
    lp = ",".join([str(p) for p in lista])
    t_personas = mi.leer_tabla('personas', filtro = f'person_id in ({lp})' )
    lista = [p for p in lista if p not in t_personas.person_id.tolist()]

  with open(detalles_log, 'a', encoding="utf-8") as f:     
    inicio = dt.now()
    f.write(f'\t {inicio} inicio {len(lista)} peticiones personas\n') 

    if len(lista)>0:
      respuestas = []
      for i, pid in enumerate(lista):
        url = f'https://api.themoviedb.org/3/person/{pid}?api_key={API_KEY}&language=en-US'
        r = requests.get(url)
        respuestas.append(r.json())    
        f.write(f'\t\t {i}  {url}\n') 
  
      personas = pd.DataFrame.from_dict(respuestas)
      personas = personas[['id', 'name', 'popularity', 'gender', 'place_of_birth', 'birthday', 'deathday', 'imdb_id']]
      personas.rename (columns={'id':'person_id'}, inplace=True)
  
      f.write(f'\t fin peticiones personas: {divmod((dt.now()-inicio).seconds, 60)}  \n') 
    
    # guardar tabla
    db = mi.conectar_bd()
    mi.guardar_tabla(personas, 'personas', db, guardar)
   
    mi.desconectar_bd(db)    
    

def extraer_creditos(df, guardar='replace'):
  """
  crea nuevos dataframes reparto y equipo a partir de la columna credits 
  y los guarda en tablas independientes  peli_reparto y peli_equipo respectivamente
  genera la lista de persona distintas para obtener sus detalles
  
  Parameters: df, dataframe con el resultado de las peticiones
              guardar, opcion para guardar la tabla (replace, append, fail)
  """    
  with open(detalles_log, 'a', encoding="utf-8") as f:   
    f.write(f'{dt.now()} inicio extraer creditos \n') 
    
  # columna credits de tipo diccionario de listas {cast:[{}], crew:[{}]} 
  # crear nuevas columnas de tipo lista de diccionarios con cada clave cast, crew 
  creditos = df[['TMDB_ID','credits']]
  creditos = creditos.join(pd.DataFrame(creditos.pop('credits').values.tolist()))
    
  #extraer lista de reparto
  reparto = expandir_caracteristica(creditos, ['TMDB_ID','cast'])
  reparto[ ['gender','id', 'cast_id','order']]=reparto[ ['gender','id', 'cast_id','order']].fillna(0).apply(lambda x: x.astype(int))
  reparto.rename (columns={'id':'person_id'}, inplace=True)

  #extraer lista de equipo técnico
  equipo = expandir_caracteristica(creditos, ['TMDB_ID','crew'])
  equipo[ ['gender','id' ]]=equipo[ ['gender','id']].fillna(0).apply(lambda x: x.astype(int))
  equipo.rename (columns={'id':'person_id'}, inplace=True)

  # guardar tablas
  db = mi.conectar_bd()
  mi.guardar_tabla(reparto, 'peli_reparto', db, guardar)
  mi.guardar_tabla(equipo, 'peli_equipo', db, guardar)
  mi.desconectar_bd(db)     

  # eliminar columna extraida
  df.drop(axis=1, columns='credits', inplace=True) 
"""

  # # lista de distintas personas más relevantes para la película  
  # reparto = reparto[reparto.order<6]
  # relevantes = ['Director', 'Director of Photography', 'Main Title Theme Composer', 'Original Music Composer', 'Producer', 'Screenplay', 'Story', 'Visual Effects', 'Writer'] 
  # equipo = equipo[equipo.job.isin(relevantes)]  
  # personas = reparto.person_id.append(equipo.person_id)
  # obtener_personas(personas.unique().tolist(), guardar )   
"""


def extraer_productoras(df, guardar='replace'):
  """
  sustituye la columna production_countries con el número de paises que producen la película
  y guarda en tablas independientes la información de productoras y paises 
  
  Parameters: df, dataframe con el resultado de las peticiones
              guardar, opcion para guardar la tabla (replace, append, fail)
  """
  with open(detalles_log, 'a', encoding="utf-8") as f:   
    f.write(f'{dt.now()} inicio extraer productoras \n') 
    
  # columna production_companies de tipo lista de diccionarios [{}]
  productoras = expandir_caracteristica(df, ['TMDB_ID','production_companies']) 
  productoras['id'] = productoras['id'].fillna(0).astype(int)
  productoras.rename (columns={'id':'prod_id'}, inplace=True)

  # columna production_countries de tipo lista de diccionarios [{iso_3166_1: , name: }]
  paises = expandir_caracteristica(df, ['TMDB_ID','production_countries'])
  coproduccion = paises.groupby('TMDB_ID', as_index=False).aggregate({'iso_3166_1':'count'})

  # actualizar columna production_countries con numero de paises en la produccion
  df.set_index('TMDB_ID', inplace=True)
  df.update(coproduccion[['TMDB_ID','iso_3166_1']].drop_duplicates().set_index('TMDB_ID').rename (columns={'iso_3166_1':'production_countries'}))
  df.reset_index(inplace=True)  
  df.production_countries = df.production_countries.astype(int)

  # guardar tablas
  db = mi.conectar_bd()
  mi.guardar_tabla(productoras, 'peli_productoras', db, guardar)
  mi.guardar_tabla(paises, 'peli_paises', db, guardar)
  mi.desconectar_bd(db)   

  # eliminar columna extraida
  df.drop(axis=1, columns='production_companies', inplace=True) 


def extraer_coleccion(df, guardar='replace'):
  """
  sustituye la columna belongs_to_collection con el id de su diccionario 
  y guarda una tabla "colecciones" con las distintas colecciones del df 

  
  Parameters: df, dataframe con el resultado de las peticiones
              guardar, opcion para guardar la tabla (replace, append, fail)
  """   
  with open(detalles_log, 'a', encoding="utf-8") as f:   
    f.write(f'{dt.now()} inicio extraer colecciones \n') 
    
  # columna belongs_to_collection de tipo diccionario {id: , name: }
  df.belongs_to_collection.fillna(0, inplace=True)
  peli_colec = df[df.belongs_to_collection!=0][['TMDB_ID','belongs_to_collection']]
  peli_colec.reset_index(drop=True, inplace=True)
  peli_colec = peli_colec.join(pd.DataFrame(peli_colec.pop('belongs_to_collection').values.tolist()))
 
  # actualizar columna belongs_to_collection con id de la coleccion
  if not peli_colec.empty :
    df.set_index('TMDB_ID', inplace=True)
    df.update(peli_colec[['TMDB_ID','id']].drop_duplicates().set_index('TMDB_ID').rename (columns={'id':'belongs_to_collection'}))
    df.reset_index(inplace=True)  
    df.belongs_to_collection=df.belongs_to_collection.astype(int)

    # guardar lista de colecciones distintas
    peli_colec['id'] = peli_colec['id'].fillna(0).astype(int)
    peli_colec.rename (columns={'id':'col_id'}, inplace=True)
    db = mi.conectar_bd()
    mi.guardar_tabla(peli_colec[['col_id','name']].drop_duplicates(), 'colecciones', db, guardar)
    mi.desconectar_bd(db) 


def extraer_keywords(df, guardar='replace'):
  """
  guarda una tabla "keywords" con las distintas keywords del df 
  y otra "peli_keywords" con las keywords por pelicula
  
  Parameters: df, dataframe con el resultado de las peticiones 
              guardar, opcion para guardar la tabla (replace, append, fail)
  """
  with open(detalles_log, 'a', encoding="utf-8") as f:   
    f.write(f'{dt.now()} inicio extraer keywords \n') 
        
  # columna keywords de tipo diccionario de lista de diccionarios {keywords:[{id: , name: }]} 
  keywords = df.keywords.apply(lambda x: x.get('keywords')).tolist()
  # lista todas las keywords 
  keywords = [item for sublist in keywords for item in sublist ]
  # df solo con distintas
  keywords = pd.DataFrame(keywords).drop_duplicates()
  keywords.rename (columns={'id':'key_id'}, inplace=True)
  
  # guardar distintas keywords en base de datos
  db = mi.conectar_bd()
    
  if guardar == 'replace':
    mi.guardar_tabla(keywords, 'keywords', db, guardar )
  elif guardar == 'append':    
    # lista de keywords para filtrar tabla                 
    kw = ",".join([str(k) for k in keywords.key_id.tolist()])
    # tabla de base de datos                     
    t_keywords = mi.leer_tabla('keywords', filtro = f'key_id in({kw})' )
    # identificar keywords existentes
    existe = keywords.merge(t_keywords, how='inner', on = 'key_id').key_id    
    # elimina del df las keywords existentes en la tabla de base de datos
    keywords.drop(axis=0,  index=keywords[keywords.key_id.isin (existe)].index  , inplace=True) 
    # guardar solo keywords nuevas                     
    if not keywords.empty : 
      mi.guardar_tabla(keywords, 'keywords', db, guardar )
                     
  mi.desconectar_bd(db) 

  # extraer diccionario keywords por pelicula y 
  peli_kw = df[['TMDB_ID','keywords']]
  peli_kw = peli_kw.join(pd.DataFrame(peli_kw.pop('keywords').values.tolist()))
  peli_kw = expandir_caracteristica(peli_kw, ['TMDB_ID','keywords'])
  peli_kw['id'] = peli_kw['id'].fillna(0).apply(  int)
  peli_kw.rename (columns={'id':'key_id'}, inplace=True)

  # guardar en base de datos
  db = mi.conectar_bd()
  mi.guardar_tabla(peli_kw[['TMDB_ID','key_id']], 'peli_keywords', db, guardar)
  mi.desconectar_bd(db)      

  # eliminar columna extraida
  df.drop(axis=1, columns='keywords', inplace=True) 

  
def obtener_detalles(df, guardar='replace'):
  """
  dada una lista de peliculas obtine la información de cada una de ellas proporcionada por la API de The Movie DaaBase
  haciendo uso del parámetro append_to_response para hacer 3 peticiones (movie, credits, keywords) en 1. 
  El resultado se guarda en la base de datos, tabla peli_detalles
  
  
  Parameters: df, dataframe con la lista de películas
              guardar, opcion para guardar la tabla (replace, append, fail)
  """
  global detalles_log
  detalles_log = f'{dt.today().strftime("%Y%m%d")}_detalles.log'
  try:
    with open(detalles_log, 'a', encoding="utf-8") as f:   
      inicio = dt.now()
      f.write(f'{inicio} inicio peticiones {df[df.TMDB_ID>0].shape[0]} películas\n') 
    
      # inicializar lista de resultados de las peticiones
      respuestas = []
      for i, peli_id in  df[df.TMDB_ID>0]['TMDB_ID'].reset_index(drop=True).items() :
        url = f'https://api.themoviedb.org/3/movie/{peli_id}?api_key={API_KEY}&append_to_response=credits,keywords'     
        r = requests.get(url)
        respuestas.append(r.json())
        f.write(f'\t {i}  {url}\n') 

      # convierte los resultados de las peticiones en dataframe 
      detalles = pd.DataFrame.from_dict(respuestas)
      detalles.rename(columns={'id':'TMDB_ID'}, inplace=True)
      detalles = detalles[['TMDB_ID','original_title','release_date','runtime','original_language','tagline','overview','belongs_to_collection','genres','credits','production_companies','production_countries','keywords','budget','revenue','imdb_id']]

      f.write(f'fin peticiones películas: {divmod((dt.now()-inicio).seconds, 60)}  \n') 
            
    ## dato inicial para poder comparar con el resultado final notebook <<<<<<<<<<
    #detalles_0 = detalles.copy(deep=True)
    
    # extrae columnas de tipo diccionario o lista para crear tablas independientes
    extraer_coleccion(detalles, guardar)
    extraer_generos(detalles, guardar)
    extraer_productoras(detalles, guardar)
    extraer_creditos(detalles, guardar)
    extraer_keywords(detalles, guardar)
   
    detalles.drop_duplicates(inplace=True)
  
    # guardar en base de datos
    db = mi.conectar_bd()
    mi.guardar_tabla(detalles, 'peli_detalles', db, guardar, True)
    mi.desconectar_bd(db) 

    with open(detalles_log, 'a', encoding="utf-8") as f:
      f.write(f'{dt.now()} FIN ')      
  except Exception as err:
    with open(detalles_log, 'a', encoding="utf-8") as f:
      f.write(f'{dt.now()} ERROR - {type(err).__name__}: {err} ')     
    #return pd.DataFrame(), pd.DataFrame() # para poder comparar en notebook <<<<<<<<<<<<<
        
  ## devolucion para poder comparar en notebook <<<<<<<<<<<<<
  #return detalles , detalles_0



if __name__ == '__main__':
  
    ### LEER PELICULAS DE BBDD
    peliculas = mi.leer_tabla('peliculas')

    ### BUSCAR PELICULA CON API DE TMDB    
    buscar_TMDB(peliculas)

    # GUARDAR RESULTADO EN BBDD
    guardar_TMDB_ID(peliculas)
    
    ### EXTRAER DETALLES DE PELICULAS CON API DE TMDB 
    ## devolucion para poder comparar en notebook <<<<<<<<<<<<<
    #peli_detalles, peli_detalles0 = obtener_detalles(peliculas)
    obtener_detalles(peliculas)
    
