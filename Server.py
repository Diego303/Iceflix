#!/usr/bin/python3 -u
# -*- coding: utf-8 -*-

import sys
import uuid
from threading import Timer
import random
import hashlib
import Ice
import IceStorm

try:
    Ice.loadSlice('iceflix.ice')
    import IceFlix
except ImportError:
    Ice.loadSlice(os.path.join(os.path.dirname(__file__), "iceflix.ice"))
    import IceFlix


class Main(IceFlix.Main):
    
    def __init__ (self, diccionario):
        self.diccionario=diccionario
        self.primera_vez=True
        self.ADMIN_PWD='admin'
        

    def isAdmin(self, message, current=None):
        return message == hashlib.sha256(self.ADMIN_PWD.encode()).hexdigest()

    def getAuthenticator(self, current=None):
        try:
            temp = random.choice(list(self.diccionario["Authenticator"]))
            self.diccionario["Authenticator"][temp].ice_ping()
            return self.diccionario["Authenticator"][temp]
        except Exception as ex:
            if len(self.diccionario["Authenticator"])==0:
                raise IceFlix.TemporaryUnavailable from ex
            else:
                self.diccionario["Authenticator"].pop(temp)
                return self.getAuthenticator()

        
    def getCatalog(self, current=None):
        try:
            temp = random.choice(list(self.diccionario["Catalog"]))
            self.diccionario["Catalog"][temp].ice_ping()
            return self.diccionario["Catalog"][temp]
        except Exception as ex:
            if len(self.diccionario["Catalog"])==0:
                raise IceFlix.TemporaryUnavailable from ex
            else:
                self.diccionario["Catalog"].pop(temp)
                return self.getCatalog()
      
    def updateDB(self, currentServices, srvId, current=None):
        if self.primera_vez:
            for auth in currentServices.authenticators:
                tmp = str(uuid.uuid4())
                self.diccionario["Authenticator"][tmp] = auth
            for cat in currentServices.mediaCatalogs:
                tmp = str(uuid.uuid4())
                self.diccionario["Catalog"][tmp] = cat
            self.primera_vez=False

class ServiceAnnouncements(IceFlix.ServiceAnnouncements):

    def __init__(self, diccionario):
        self.diccionario=diccionario
    
    @property
    def known_services(self):
        """Get serviceIds for all services"""
        return list(self.diccionario["Authenticator"].keys()) + list(self.diccionario["Catalog"].keys()) + list(self.diccionario["Main"].keys())

    #Esto funcionara cuando nos llega algo de announcements
    def newService(self, service, srvId, current=None):
        """Check service type and add it."""
        if srvId in self.known_services:
            return
        if service.ice_isA('::IceFlix::Authenticator'):
            print(f'New Authenticator service: {srvId}')
            self.diccionario["Authenticator"][srvId] = IceFlix.AuthenticatorPrx.uncheckedCast(service)
        elif service.ice_isA('::IceFlix::MediaCatalog'):
            print(f'New MediaCatalog service: {srvId}')
            self.diccionario["Catalog"][srvId] = IceFlix.MediaCatalogPrx.uncheckedCast(service)
        elif service.ice_isA('::IceFlix::Main'):
            """Si es un main nuevo tenemos que actualizar la BD"""
            print(f'New Main service: {srvId}')
            self.diccionario["Main"][srvId] = IceFlix.MainPrx.uncheckedCast(service)
            new_main = IceFlix.MainPrx.uncheckedCast(service)
            serv_volatiles = IceFlix.VolatileServices()
            serv_volatiles.authenticators = list(self.diccionario["Authenticator"].values())
            serv_volatiles.mediaCatalogs = list(self.diccionario["Catalog"].values())
            new_main.updateDB(serv_volatiles,srvId)

    def announce(self, service, srvId, current=None):
        """Check service type and add it."""
        if srvId in self.known_services:
            return
        if service.ice_isA('::IceFlix::Authenticator'):
            print(f'Announce Authenticator service: {srvId}')
            self.diccionario["Authenticator"][srvId] = IceFlix.AuthenticatorPrx.uncheckedCast(service)
        elif service.ice_isA('::IceFlix::MediaCatalog'):
            print(f'Announce MediaCatalog service: {srvId}')
            self.diccionario["Catalog"][srvId] = IceFlix.MediaCatalogPrx.uncheckedCast(service)
        elif service.ice_isA('::IceFlix::Main'):
            self.diccionario["Main"][srvId] = IceFlix.MainPrx.uncheckedCast(service)
            print(f'Announce Main service: {srvId}')

class Server(Ice.Application):


    def get_topic_manager(self):
        key = 'IceStorm.TopicManager.Proxy'
        proxy = self.communicator().propertyToProxy(key)
        if proxy is None:
            print("property '{}' not set".format(key))
            return None

        print("Using IceStorm in: '%s'" % key)
        return IceStorm.TopicManagerPrx.checkedCast(proxy)

    def anunciar(self, announcements, ms, srvId, timer, diccionario, current=None):
        announcements.announce(ms, srvId)
        print("\n##########################proxy server.py######################\n",ms)

        auths_invalidos=[]
        for obj in diccionario["Authenticator"]:
            try:
                diccionario["Authenticator"][obj].ice_ping()
            except Exception as ex:
                auths_invalidos.append(obj)

        for obj in auths_invalidos:
            diccionario["Authenticator"].pop(obj)

        catalog_invalidos=[]
        for obj in diccionario["Catalog"]:
            try:
                diccionario["Catalog"][obj].ice_ping()
            except Exception as ex:
                catalog_invalidos.append(obj)
        for obj in catalog_invalidos:
            diccionario["Catalog"].pop(obj)

        main_invalidos=[]
        for obj in diccionario["Main"]:
            try:
                diccionario["Main"][obj].ice_ping()
            except Exception as ex:
                main_invalidos.append(obj)
        for obj in main_invalidos:
            diccionario["Main"].pop(obj)

        timer = Timer((10.00+random.uniform(-2.00,2.00)),self.anunciar,(announcements, ms,srvId,timer, diccionario))
        timer.start()

    def run(self, argv):
        diccionario= {"Authenticator":{},"Catalog":{},"Main":{}}
        servMain = Main(diccionario)
        if len(argv)==2:
            password_admin = argv[1]
        else:
            password_admin = ''
            
        if servMain.isAdmin(hashlib.sha256(password_admin.encode()).hexdigest()):
            topic_mgr = self.get_topic_manager()
            if not topic_mgr:
                print("Invalid proxy")
                return 2

             
            broker = self.communicator()

            
            servAnnouncements = ServiceAnnouncements(diccionario)
            adapter = broker.createObjectAdapter("MainAdapter")
            ms_announcements = adapter.addWithUUID(servAnnouncements)#micro servicio de announcements

            topic_name = "serviceannouncements"

            qos = {}
            try:
                topic = topic_mgr.retrieve(topic_name)
            except IceStorm.NoSuchTopic:
                topic = topic_mgr.create(topic_name)

            topic.subscribeAndGetPublisher(qos, ms_announcements)
            print("Waiting events... '{}'".format(ms_announcements))

            publisher = topic.getPublisher()
            announcements = IceFlix.ServiceAnnouncementsPrx.uncheckedCast(publisher)
            
            ms_main = adapter.addWithUUID(servMain)#micro servicio de main
            adapter.activate()
            print("---Iniciado servicio main")
            identificador = uuid.uuid4()
            diccionario["Main"][str(identificador)] = ms_main
            announcements.newService(ms_main, str(identificador))
            timer = None
            timer = Timer(3.00,self.anunciar,(announcements, ms_main, str(identificador),timer, diccionario))
            timer.start()
            
            self.shutdownOnInterrupt()
            broker.waitForShutdown()
            try:
                topic.unsubscribe(subscriber)
            except:
                print("\nSaliendo del servicio main")
        else:
            print("Contrasena incorrecta, no se ha iniciado el servidor")
        return 0

    

server = Server()
sys.exit(server.main(sys.argv))
