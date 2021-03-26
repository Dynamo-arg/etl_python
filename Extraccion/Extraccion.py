#!/usr/bin/env python

from __future__ import print_function
import meli
from meli.rest import ApiException
from pprint import pprint
import json
import requests
import tinymongo as tm
import tinydb
import numpy as np
import os
import bonobo

# Bug: https://github.com/schapman1974/tinymongo/issues/58
class TinyMongoClient(tm.TinyMongoClient):
    @property
    def _storage(self):
        return tinydb.storages.JSONStorage

db_name = 'competencia'

def extract():

    # Conectar con mercado libre
    configuration = meli.Configuration(
        host = "https://api.mercadolibre.com"
        )

    with meli.ApiClient() as api_client:

        api_instance = meli.OAuth20Api(api_client)
        grant_type = 'refresh_token' # or 'refresh_token' if you need get one new token
        client_id = '1168054310416447' 
        client_secret = 'kdxgHYxI8enxKZOVUdazyzq3VabrqD8D' 
        redirect_uri = 'https://mercadolibre.com.ar' 
        code = '' # The parameter CODE, empty if your send a refresh_token
        refresh_token = 'TG-605354c8c3aa2e0008d4a35d-184827283' # Your refresh_token

    try:                        
        api_response = api_instance.get_token(grant_type=grant_type, client_id=client_id, client_secret=client_secret, redirect_uri=redirect_uri, code=code, refresh_token=refresh_token)
        pprint(api_response)

    except ApiException as e:
        print("Excepción al llamar OAuth20Api->get_token: %s\n" % e)                 

    # Almaceno Access Token
    tok = str("Bearer ") + api_response.get("access_token")
    url_contador = "https://api.mercadolibre.com/sites/MLA/search?nickname=VINOTECA+BARI"
    publica = requests.post(url_contador).json()  
    conteo = publica['paging']['total']

    for i in range (0,conteo,50):
        url = "https://api.mercadolibre.com/sites/MLA/search?nickname=VINOTECA+BARI" + "&offset=" + str(i)
        headers = {"Authorization": tok}

        res = requests.post(url, headers=headers).json()
        results = res["results"] 
        
        yield results


def transform(results):
    
    dataset = [
        {"id":x.get("id"),
        "title":x.get("title"),
        "price":x.get("price"),
        "available_quantity":x.get("available_quantity"),
        "sold_quantity":x.get("sold_quantity")
        } 
        for x in results
        ]
        
    yield dataset

def load(dataset):
    conn = TinyMongoClient()
    db = conn[db_name]

    db.usuario.insert_many(dataset)
    conn.close()





def get_graph(**options):
    graph = bonobo.Graph()
    graph.add_chain(extract, transform, load)
    return graph


def get_services(**options):
    return {}


if __name__ == "__main__":
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )