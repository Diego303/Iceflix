#!/usr/bin/python3 -u
# -*- coding: utf-8 -*-

import sys
import uuid
from threading import Timer
import random
import json
import secrets
import Ice
import IceStorm

try:
    Ice.loadSlice('iceflix.ice')
    import IceFlix
except ImportError:
    Ice.loadSlice(os.path.join(os.path.dirname(__file__), "iceflix.ice"))
    import IceFlix


class Authenticator(IceFlix.Authenticator):
    def __init__(self, diccionario, users_passwords, tokens_users, user_updates, revocations, srvId):
        self.tokens_users = tokens_users
        self.diccionario = diccionario
        self.users_passwords = users_passwords
        self.user_updates=user_updates
        self.revocations=revocations
        self.srvId=srvId
        self.primera_vez=True
    
    def get_main(self, current=None):
        try:
            temp = random.choice(list(self.diccionario["Main"]))
            self.diccionario["Main"][temp].ice_ping()
            return self.diccionario["Main"][temp]
        except Exception as ex:
            if len(self.diccionario["Main"])==0:
                raise IceFlix.TemporaryUnavailable from ex
            else:
                self.diccionario["Main"].pop(temp)
                return self.get_main()

    def refreshAuthorization(self, user, passwordHash, current=None):
        
        try:
            password = self.users_passwords[user]
        except KeyError:
            raise IceFlix.Unauthorized
        if password != passwordHash:
            raise IceFlix.Unauthorized
        token = secrets.token_urlsafe(16)
        self.user_updates.newToken(user, token, self.srvId)
        timer = Timer(120.0,self.revocar_token,(token,None))
        timer.start()
        return token

    def revocar_token(self, token, current=None):
        self.revocations.revokeToken(token, self.srvId)

    def isAuthorized(self, userToken, current=None):
        try:
            tmp=self.tokens_users[userToken]
            return True
        except KeyError as e:
            return False

    def whois(self, userToken, current=None):
        if self.isAuthorized(userToken):
            return self.tokens_users[userToken]
        else:
            raise IceFlix.Unauthorized

    def addUser(self, user, passwordHash, adminToken, current=None):
        ms_main = self.get_main()
        if ms_main.isAdmin(adminToken):
            self.user_updates.newUser(user, passwordHash, self.srvId)
            with open("users.json",'r+') as db:
                tmp = json.load(db)
                tmp.update(self.users_passwords)
                tmp[user]=passwordHash

                db.seek(0)
                json.dump(tmp,db)
        else:
            raise IceFlix.Unauthorized

    def removeUser(self, user, adminToken, current=None):
        ms_main = self.get_main()
        if ms_main.isAdmin(adminToken):
            self.revocations.revokeUser(user, self.srvId)
            with open("users.json",'r+') as db:
                tmp = json.load(db)
                tmp.update(self.users_passwords)
                tmp.pop(user)
                db.seek(0)
                json.dump(tmp,db)
                db.truncate()
        else:
            raise IceFlix.Unauthorized

    def updateDB(self, currentDatabase, srvId, current=None):
        if self.primera_vez:
            self.users_passwords=currentDatabase.userPasswords
            self.tokens_users=currentDatabase.usersToken
            self.primera_vez=False

class UserUpdates(IceFlix.UserUpdates):
    def __init__(self,users_passwords, tokens_users):
        self.users_passwords=users_passwords
        self.tokens_users=tokens_users

    def newUser(self, user, passwordHash, srvId, current=None):
        self.users_passwords[user]=passwordHash

    def newToken(self, user, userToken, srvId, current=None):
        self.tokens_users[userToken] = user

class Revocations(IceFlix.Revocations):
    def __init__(self, users_passwords, tokens_users):
        self.users_passwords=users_passwords
        self.tokens_users=tokens_users 

    def revokeToken(self, userToken, srvId, current=None):
        self.tokens_users.pop(userToken)

    def revokeUser(self, user, srvId, current=None):
        self.users_passwords.pop(user)

class ServiceAnnouncements(IceFlix.ServiceAnnouncements):

    def __init__(self, diccionario, users_passwords, tokens_users):
        self.diccionario=diccionario
        self.users_passwords=users_passwords
        self.tokens_users=tokens_users
    
    @property
    def known_services(self):
        """Get serviceIds for all services"""
        return list(self.diccionario["Authenticator"].keys()) + list(self.diccionario["Catalog"].keys()) + list(self.diccionario["Main"].keys())

    
    def newService(self, service, srvId, current=None):
        """Check service type and add it."""
        if srvId in self.known_services:
            return
        if service.ice_isA('::IceFlix::MediaCatalog'):
            print(f'New MediaCatalog service: {srvId}')
            self.diccionario["Catalog"][srvId] = IceFlix.MediaCatalogPrx.uncheckedCast(service)
        elif service.ice_isA('::IceFlix::Main'):
            print(f'New Main service: {srvId}')
            self.diccionario["Main"][srvId] = IceFlix.MainPrx.uncheckedCast(service)
        elif service.ice_isA('::IceFlix::Authenticator'):
            """Si es un Authenticator nuevo tenemos que actualizar la BD"""
            print(f'New Authenticator service: {srvId}')
            self.diccionario["Authenticator"][srvId] = IceFlix.AuthenticatorPrx.uncheckedCast(service)
            new_authenticator = IceFlix.AuthenticatorPrx.uncheckedCast(service)
            users_db = IceFlix.UsersDB()
            users_db.userPasswords = self.users_passwords
            users_db.usersToken = self.tokens_users
            new_authenticator.updateDB(users_db,srvId)
            

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

class AuthenticatorService(Ice.Application):
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
        topic_mgr = self.get_topic_manager()
        if not topic_mgr:
            print("Invalid proxy")
            return 2

        diccionario= {"Authenticator":{},"Catalog":{},"Main":{}}
        users_passwords= {}
        with open('users.json', 'r') as contents:
        	users_passwords = json.load(contents)
        tokens_users = {}
        broker = self.communicator()

        servAnnouncements = ServiceAnnouncements(diccionario, users_passwords, tokens_users)
        adapter = broker.createObjectAdapter("AuthenticatorAdapter")
        ms_announcements = adapter.addWithUUID(servAnnouncements)

        topic_name = "serviceannouncements"

        qos = {}
        try:
            topic = topic_mgr.retrieve(topic_name)
        except IceStorm.NoSuchTopic:
            topic = topic_mgr.create(topic_name)

        topic.subscribeAndGetPublisher(qos, ms_announcements)
        print("Waiting events... '{}'".format(ms_announcements))
        """Suscriptor userupdates"""
        user_updates = UserUpdates(users_passwords, tokens_users)
        ms_user_updates = adapter.addWithUUID(user_updates)

        topic_name = "userupdates"

        qos = {}
        try:
            topic_uu = topic_mgr.retrieve(topic_name)
        except IceStorm.NoSuchTopic:
            topic_uu = topic_mgr.create(topic_name)

        topic_uu.subscribeAndGetPublisher(qos, ms_user_updates)
        """Suscriptor revocations"""
        revocations = Revocations(users_passwords, tokens_users)
        ms_revocations = adapter.addWithUUID(revocations)

        topic_name = "revocations"

        qos = {}
        try:
            topic_r = topic_mgr.retrieve(topic_name)
        except IceStorm.NoSuchTopic:
            topic_r = topic_mgr.create(topic_name)

        topic_r.subscribeAndGetPublisher(qos, ms_revocations)

        """Publicador announcements"""
        publisher = topic.getPublisher()
        announcements = IceFlix.ServiceAnnouncementsPrx.uncheckedCast(publisher)
        """Publicador user updates"""
        publisher_uu = topic_uu.getPublisher()
        announcements_uu = IceFlix.UserUpdatesPrx.uncheckedCast(publisher_uu)
        """Publicador revocations"""
        publisher_r = topic_r.getPublisher()
        announcements_r = IceFlix.RevocationsPrx.uncheckedCast(publisher_r)

        identificador = uuid.uuid4()
        
        servAuthenticator = Authenticator(diccionario, users_passwords, tokens_users, announcements_uu, announcements_r, str(identificador))
        ms_authenticator = adapter.addWithUUID(servAuthenticator)
        diccionario["Authenticator"][str(identificador)] = ms_authenticator
        adapter.activate()

        print("---Authenticator iniciado")
        announcements.newService(ms_authenticator, str(identificador))
        timer = None
        timer = Timer(3.00,self.anunciar,(announcements, ms_authenticator, str(identificador),timer, diccionario))
        timer.start()
        
        self.shutdownOnInterrupt()
        broker.waitForShutdown()
        try:
            topic.unsubscribe(subscriber)
        except:
            print("\nSaliendo del servicio Authenticator")
        return 0

authenticatorService = AuthenticatorService()
sys.exit(authenticatorService.main(sys.argv))
