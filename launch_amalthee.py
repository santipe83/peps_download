#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import logging
import requests
import base64
import time
import re
import xml.etree.ElementTree as ET

REGXEP_WPS_ID = re.compile("report\-.*\.")

def get_amalthee_url(opensearch_request):
    """
    Construit la requête de lancement d'AMALTHEE
    :param opensearch_request:
    :return:
    """
    opensearch_request_b64 = base64.b64encode(opensearch_request)
    request = "http://peps-vizo.cnes.fr:8081/cgi-bin/pywps.cgi?request=execute&" \
              "service=WPS&version=1.0.0&identifier=AMALTHEE&datainputs=opensearch_request=" + \
              opensearch_request_b64 + "&status=true&storeExecuteResponse=true"
    return request

def get_status_url(wps_id):
    """
    Construit la requête de mise à jour du rapport json
    :param wps_id: identifiant WPS de l'instance AMALTHEE
    :return: chaîne de caractère représentant la requête complète
    """
    request = "http://peps-vizo.cnes.fr:8081/cgi-bin/pywps.cgi?request=execute&service=WPS&" \
              "version=1.0.0&identifier=STATUS&" \
              "datainputs=wps_id=" + wps_id + "&status=false&storeExecuteResponse=false"
    return request

def get_json_url(wps_id):
    request = "http://peps-vizo.cnes.fr:8081/wps/outputs/report-" + wps_id + ".json"
    return request


if __name__ == "__main__":
    '''
    Ce code permet de lancer la commande WPS de rapatriement des données PEPS dans le tampon cluster
    '''
    # Description :
    # 1) Encodage de la requête opensearch en base64
    # 2) Appel au service WPS AMALTHEE avec comme argument la requête encodée
    # 3) Analyse du XML de sortie pour récupérer :
    #    - l'ID WPS,
    #    - le lien vers le rapport JSON
    # 4) Affichage du lien et de la commande à exécuter pour mettre à jour le rapport

    # Récupération des arguments
    description = "Lance la requête WPS qui permet de faire une recherche Opensearch de " \
                  "produits PEPS et de les rapatrier vers le cluster HAL dans le répertoire" \
                  "/work/OT/peps/products/"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("request", type=str, help="Requête Opensearch ou identifiant WPS")
    parser.add_argument("-l", "--logs", help="Fichier de destination des logs")
    args = parser.parse_args()

    # Définition de la sortie de log
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO, filename=args.logs)

    if args.request[0:4] == "http":
        # Lancer AMALTHEE web service => job sur le cluster
        url_amalthee = get_amalthee_url(args.request)
        logging.debug("Requête URL : {}".format(url_amalthee))
        request_amalthee = requests.get(url_amalthee)
        logging.debug(request_amalthee.text)

        if request_amalthee.status_code != 200:
            raise Exception("Request error : {}".format(str(request_amalthee.status_code)))

        # Trouver statusLocation
        xml_amalthee = ET.fromstring(request_amalthee.text)
        status_location = xml_amalthee.attrib["statusLocation"]

        # Attente de deux secondes pour que WPS renvoie un statut correct
        # et trouver le rapport json
        time.sleep(2)
        request_status = requests.get(status_location)
        xml_status = ET.fromstring(request_status.text)
        json_report = xml_status[2][0][2].attrib["href"]

        # Affichage du rapport
        wps_id = REGXEP_WPS_ID.findall(json_report)[0][7:-1]
        print("Le rapport d'exécution est visible à l'adresse : {}".format(json_report))
        print("Pour le mettre à jour, lancer la commande :")
        print("python launch_amalthee.py {}".format(wps_id))
    else:
        request_status = requests.get(get_status_url(args.request))
        if request_status.status_code != 200:
            raise Exception("Request error : {}".format(str(request_status.status_code)))
        else:
            if "Failed" in request_status.text:
                raise ValueError("L'ID WPS {} n'est pas connu".format(args.request))
            print("Le fichier json a été mis à jour.")