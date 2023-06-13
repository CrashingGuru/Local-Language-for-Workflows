#Created: 13 June 2023
#Author: Vishnu Ram OV.   
#vishnu.n@ieee.org.   
#Licence: Apache 2.0 

#CAUTION
#this runs inside the container spawned by github actions
#expects magik file issue.out in the same dir as this py script.
#Called from the workflow survey-sample-2023.yml


import os
import json

import subprocess
import sys

reqs = subprocess.check_output([sys.executable, '-m', 'pip',
'freeze'])
installed_packages = [r.decode().split('==')[0] for r in reqs.split()]

print(installed_packages)
print(sys.path)

from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable

class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    #Vishnu: 1 Aug 2022: uses labels instead of properties
    #Vishnu: 1 Aug 2022: Invariant: only 1 actor with 1 name.
    #        (Note that rel still uses properties and not labels)
    def create_node_with_usecase_label(self, actor_name, usecase_id):
        with self.driver.session() as session:
            node_already_exists = session.read_transaction(
                self._find_and_return_existing_node_label, actor_name)
            if not node_already_exists:  
                #node itself doesnt exist (not to mention the use case label), Lets add.
                result = session.write_transaction(
                    self._create_and_return_node_label, actor_name, usecase_id)
                for row in result:
                    print("Created node: {n1}".format(n1=row['n1name']))
            else:
                for row in node_already_exists:
                    print("Node already exists: {name} in {usecase}".format(name=row['n1name'], 
                                                                 usecase=row['n1usecase']))
                    if not (usecase_id in row['n1usecase']):
                      print("adding label "
                                +usecase_id+ 
                                " in addition to {usecase}".format(usecase=row['n1usecase']))
                      result = session.write_transaction(
                                self._add_usecase_and_return_existing_node_label, actor_name, usecase_id)
                    else:
                      print("ignoring ...")
                    #row has only 1 entry due to invariant. So break here. and exit f()
                    break
    
    #Vishnu: 1 Aug 2022: created this f() to query nodes based on a name
    #                    and return with usecase labels
    @staticmethod
    def _find_and_return_existing_node_label(tx, actor_name):
        query = (
            "MATCH (n1) "
            "WHERE (n1.name ='" + actor_name + "') "
            "RETURN n1.name as n1name, labels(n1) as n1usecase"
        )
        result = tx.run(query)
        try:
            return [{"n1name": row["n1name"],"n1usecase": row["n1usecase"]} 
                    for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    #Vishnu: 1 Aug 2022: created this f() to create nodes with usecase label
    @staticmethod
    def _create_and_return_node_label(tx, actor_name, usecase_id):
            query = (
            "CREATE (n1 :" + usecase_id + " { name: '" + actor_name + "'}) "
            "RETURN n1"
            )
            result = tx.run(query, actor_name=actor_name)
            try:
                return [{"n1name": row["n1"]["name"]}
                        for row in result]
            # Capture any errors along with the query and data for traceability
            except ServiceUnavailable as exception:
                logging.error("{query} raised an error: \n {exception}".format(
                    query=query, exception=exception))
                raise

    #Vishnu: 1 Aug 2022: created this f() to add use case labels to existing nodes
    #                    Note that the existing labels are left there.
    @staticmethod
    def _add_usecase_and_return_existing_node_label(tx, actor_name, usecase_id):
        query = (
            "MATCH "
            "(n1) "
            "WHERE n1.name = '" + actor_name +"' "
            "set n1 :" + usecase_id +" "
            "return n1"
        )
        result = tx.run(query, actor_name=actor_name)
        try:
            return [{"n1": row["n1"]["name"]}
                    for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    #Vishnu: 1 Aug 2022: created this f() to return all use case actors based on labels
    #                    as against properties
    @staticmethod
    def _find_and_return_all_usecase_actors_label(tx, usecase_id):
        query = (
            "MATCH (n) "
            "WHERE '" +usecase_id+ "' in labels(n) "
            "RETURN n.name AS name"
        )
        result = tx.run(query)
        return [row["name"] for row in result]

    #Vishnu: 1 Aug 2022: created this f() to list all nodes with usecase label
    #                    instead of properties
    def find_all_usecase_actors_label(self, usecase_id):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_all_usecase_actors_label, usecase_id)
            i=0
            for row in result:
                i=i+1
                print("Found actor-"+ str(i) +": {row}".format(row=row))

    #Vishnu: 1 Aug 2022: Created to use label for use case.
    #CAUTION: Assumes that actors already exists with label as use case.
    #So, this has to be called only in conjunction with create_node_with_usecase
    #otherwise you may end up with actors not having use case prop
    #but rels will have use case prop. not a good idea.
    def create_rel_with_usecase_label(self, actor1_name, rel_name, actor2_name, usecase_id):
        with self.driver.session() as session:
            this_rel_already_exists = session.read_transaction(
                self._find_and_return_this_existing_rels, actor1_name, rel_name, actor2_name, usecase_id)
            if (this_rel_already_exists):
                print(rel_name + " already exists with "+ usecase_id + " between "+ actor1_name + " and "+ actor2_name)
                return
            else:
                #we are pretty sure something didnt match
                #this relation does not exist currently between the nodes under the use case, 
                #add it.
                result3 = session.write_transaction(
                    self._create_and_return_rel_label, actor1_name, rel_name, actor2_name, usecase_id)
                #if (not retult3)
                for row in result3:
                    print("Created relation: {actor1} - {rel} - {actor2} in {usecase}".
                            format(actor1=row['n1name'], 
                            rel=row['r1name'],
                            actor2=row['n2name'],
                            usecase=row['r1usecase']))

    #Vishnu: 1 Aug 2022 
    #assumption: nodes already exist
    #modified from _create_and_return_rel to use labels for n.usecase
    @staticmethod
    def _create_and_return_rel_label(tx, actor1_name, rel_name, actor2_name, usecaseid):
            query = (
            "MATCH "
            "(n1), "
            "(n2) "
            "WHERE n1.name = '" + actor1_name + "' AND n2.name = '"+actor2_name+"' "
            " AND '"+ usecaseid+"' in labels(n1) "
            " AND '"+ usecaseid+"' in labels(n2) "
            "CREATE (n1)-[r1:Relation " + "{name: '"+rel_name+"', usecase: '"+usecaseid+"'} ]->(n2) "
            "RETURN n1, r1, n2"
            )
            result = tx.run(query)
            try:
                return [{"n1name": row["n1"]["name"], 
                        "r1name": row["r1"]["name"],
                        "n2name": row["n2"]["name"],
                        "r1usecase": row["r1"]["usecase"]}
                        for row in result]
            # Capture any errors along with the query and data for traceability
            except ServiceUnavailable as exception:
                logging.error("{query} raised an error: \n {exception}".format(
                    query=query, exception=exception))
                raise

    #Vishnu: 1 Aug 2022: Deprecated: This approach uses properties instead of labels for n.usecases.
    #Vishnu: 1 Aug 2022: Invariant: only 1 actor with 1 name.
    #Vishnu: 22 Jun 2022: refactored, uses properties instead of labels
    #Vishnu: 1 Jun 2022: created this f() to create nodes with usecase label
    #Vishnu: 2 Jun 2022: added- check if it already exists.
    #Vishnu: 2 Jun 2022: added- check the usecase duplication, adds a usecase if it doesnt exist.
    #otherwise ignores the command.
    def create_node_with_usecase(self, actor_name, usecase_id):
        with self.driver.session() as session:
            node_already_exists = session.read_transaction(
                self._find_and_return_existing_node, actor_name)
            if not node_already_exists:  
                result = session.write_transaction(
                    self._create_and_return_node, actor_name, usecase_id)
                for row in result:
                    print("Created node: {n1}".format(n1=row['n1name']))
            else:
                for row in node_already_exists:
                    print("Node already exists: {name} in {usecase}".format(name=row['n1name'], 
                                                                 usecase=row['n1usecase']))
                    s=row['n1usecase'].split(',')
                    if not (usecase_id in s):
                      print("adding prop "+usecase_id+ " in addition to "+row['n1usecase'])
                      result = session.write_transaction(
                                self._add_usecase_and_return_existing_node, actor_name, usecase_id)
                    else:
                      print("ignoring ...")
                    #row has only 1 entry due to invariant. So break here. and exit f()
                    break

    #Vishnu: 1 Aug 2022: Deprecated: This approach uses properties instead of labels for usecases.
    #Vishnu: 2 Jun 2022: created this f() to query nodes and return with usecase prop
    @staticmethod
    def _find_and_return_existing_node(tx, actor_name):
        query = (
            "MATCH (n1) "
            "WHERE (n1.name ='" + actor_name + "') "
            "RETURN n1.name as n1name, n1.usecase as n1usecase"
        )
        result = tx.run(query)
        try:
            return [{"n1name": row["n1name"],"n1usecase": row["n1usecase"]} 
                    for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    #Vishnu: 1 Aug 2022: Deprecated: This approach uses properties instead of labels for usecases.
    @staticmethod
    def _create_and_return_node(tx, actor_name, usecase_id):
            query = (
            "CREATE (n1 "+"{ name: '"+actor_name+"', usecase: '"+usecase_id+"'}) "
            "RETURN n1"
            )
            result = tx.run(query, actor_name=actor_name)
            try:
                return [{"n1name": row["n1"]["name"]}
                        for row in result]
            # Capture any errors along with the query and data for traceability
            except ServiceUnavailable as exception:
                logging.error("{query} raised an error: \n {exception}".format(
                    query=query, exception=exception))
                raise

    #Vishnu: 1 Aug 2022: Deprecated: This approach uses properties instead of labels for usecases.
    #Vishnu: 1 Jun 2022: created this f() to add use case prop to existing nodes 
    #CAUTION: does not check for duplication, use create_node_with_prop instead.
    def add_usecase_existing_node(self, actor_name, usecase_id):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(
                self._add_usecase_and_return_existing_node, actor_name, usecase_id)
            
            # we expect only 1 node. Do we need the for loop below?
            for row in result:
                print("added usecase to node: {n1}".format(n1=row['n1']))

    #Vishnu: 1 Aug 2022: Deprecated: This approach uses properties instead of labels for usecases.
    @staticmethod
    def _add_usecase_and_return_existing_node(tx, actor_name, usecase_id):
        # To learn more about the Cypher syntax, see https://neo4j.com/docs/cypher-manual/current/
        # The Reference Card is also a good resource for keywords https://neo4j.com/docs/cypher-refcard/current/
        query = (
            "MATCH "
            "(n1) "
            "WHERE n1.name = '" + actor_name +"' "
            "set n1.usecase = n1.usecase + '" + "," + "'"+ "+ '"+usecase_id +"' "
            "return n1"
        )
        result = tx.run(query, actor_name=actor_name)
        try:
            return [{"n1": row["n1"]["name"]}
                    for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    #Vishnu: 1 Aug 2022: Deprecated: This approach uses properties instead of labels for usecases.
    #CAUTION: Assumes that actors already exists with property as n.use case.
    #So, this has to be called only in conjunction with create_node_with_usecase
    #otherwise you may end up with actors not having use case prop
    #but rels will have use case prop. not a good idea.
    def create_rel_with_usecase(self, actor1_name, rel_name, actor2_name, usecase_id):
        with self.driver.session() as session:
            this_rel_already_exists = session.read_transaction(
                self._find_and_return_this_existing_rels, actor1_name, rel_name, actor2_name, usecase_id)
            if (this_rel_already_exists):
                print(rel_name + " already exists with "+ usecase_id + " between "+ actor1_name + " and "+ actor2_name)
                return
            else:
                #we are pretty sure something didnt match
                #this relation does not exist currently between the nodes under the use case, 
                #add it.
                result3 = session.write_transaction(
                    self._create_and_return_rel, actor1_name, rel_name, actor2_name, usecase_id)
                #if (not retult3)
                for row in result3:
                    print("Created relation: {actor1} - {rel} - {actor2} in {usecase}".
                            format(actor1=row['n1name'], 
                            rel=row['r1name'],
                            actor2=row['n2name'],
                            usecase=row['r1usecase']))
    
    #Vishnu: 1 Aug 2022: at this moment this f() uses labels instead of properties for n.usecases.
    #                    Note that rels still uses properties.
    #        1 Aug 2022: TBD: create a property-version (as against label) of the same thing.
    #Vishnu: 21 Jun 2022: created this f() to create relationships with usecase label
    #if this works, this is the only function that needed to be called.
    def create_actors_relationship_with_usecase(self, actor1_name, rel_name, actor2_name, usecase_id):
            #process actor1
            self.create_node_with_usecase_label(actor1_name, usecase_id)
            #process actor2
            self.create_node_with_usecase_label(actor2_name, usecase_id)
            #process relation
            self.create_rel_with_usecase_label(actor1_name, rel_name, actor2_name, usecase_id)

    #Vishnu: 23 June 2022: created
    #looks for a rel under a use case between 2 given nodes.
    #normally, we expect only 0 or 1 unique entry.
    @staticmethod                
    def _find_and_return_this_existing_rels(tx, actor1_name, rel_name, actor2_name, usecase_id):
        query = (
            "MATCH (n1)-[r1]-(n2) "
            "WHERE (n1.name ='" + actor1_name + "') "
            "and   (n2.name ='" + actor2_name + "') "
            "and   (r1.usecase CONTAINS '"+ usecase_id+ "') "
            "and   (r1.name ='" + rel_name + "') "
            "RETURN n1.name as n1name, r1.name as r1name, n2.name as n2name, r1.usecase as r1usecase"
        )
        result = tx.run(query)
        return [row["r1name"] for row in result]
        
    
    #Vishnu: 21 June 2022: created this supporting f()
    #return for all relationship between given two nodes.
    @staticmethod                
    def _find_and_return_all_existing_rels_for_uc(tx, actor1_name, actor2_name, usecase_id):
        query = (
            "MATCH (n1)-[r1]-(n2) "
            "WHERE (n1.name ='" + actor1_name + "') "
            "and   (n2.name ='" + actor2_name + "') "
            "and   (r1.usecase CONTAINS '"+ usecase_id+ "') "
            "RETURN n1.name as n1name, r1.name as r1name, n2.name as n2name, r1.usecase as r1usecase"
        )
        result = tx.run(query)
        try:
            return [{
                        "n1name": row["n1name"],
                        "n2name": row["n2name"],
                        "r1name": row["r1name"],
                        "r1usecase": row["r1usecase"],
                    } 
                    for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    #Vishnu: 21 June 2022: created this supporting f()
    #adds a name for an existing relationship between given two nodes.
    @staticmethod                
    def _add_usecase_and_return_existing_rel(tx, actor1_name, rel_name, actor2_name, usecase_id):
        query = (
            "MATCH (n1)-[r1]-(n2) "
            "WHERE (n1.name ='" + actor1_name + "') "
            "and   (n2.name ='" + actor2_name + "')"
            "set    r1.usecase = r1.usecase + '" + "," + "'"+ "+ '"+usecase_id +"' "
        )
        result = tx.run(query)

    
    #Vishnu: 30 june 2022: created
    #CAUTION: if the property already exists, 
    #this overwrites the current value.
    @staticmethod
    def _write_property_and_return_existing_node(tx, actor_name, propertyName, propertyVal):
        query = (
            "MATCH "
            "(n1) "
            "WHERE n1.name = '" + actor_name +"' "
            "set n1."+propertyName+" = '"+propertyVal +"' "
            "return n1"
        )
        result = tx.run(query, actor_name=actor_name)
        try:
            return [{"n1": row["n1"]["name"]}
                    for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    #Vishnu: 30 june 2022: created
    #CAUTION: if the property already exists, 
    #this overwrites the current value.
    @staticmethod
    def _write_property_and_return_existing_rel(tx, actor1_name, rel_name, actor2_name, 
                                                propertyName, propertyVal):
        query = (
            "MATCH (n1)-[r1]-(n2) "
            "WHERE (n1.name ='" + actor1_name + "') "
            "and   (n2.name ='" + actor2_name + "') "
            "and   (r1.name ='" + rel_name + "') "
            "set   r1."+propertyName+" = '"+propertyVal +"' "
            "return r1"
        )
        result = tx.run(query)
        try:
            return [{"r1": row["r1"]["name"]}
                    for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    #Vishnu: 22 June 2022: created
    #assumption: nodes already exist
    @staticmethod
    def _create_and_return_rel(tx, actor1_name, rel_name, actor2_name, usecaseid):
            query = (
            "MATCH "
            "(n1), "
            "(n2) "
            "WHERE n1.name = '" + actor1_name + "' AND n2.name = '"+actor2_name+"' "
            " AND n1.usecase CONTAINS '"+ usecaseid+"' "
            " AND n2.usecase CONTAINS '"+ usecaseid+"' "
            "CREATE (n1)-[r1:Relation " + "{name: '"+rel_name+"', usecase: '"+usecaseid+"'} ]->(n2) "
            "RETURN n1, r1, n2"
            )
            result = tx.run(query)
            try:
                return [{"n1name": row["n1"]["name"], 
                        "r1name": row["r1"]["name"],
                        "n2name": row["n2"]["name"],
                        "r1usecase": row["r1"]["usecase"]}
                        for row in result]
            # Capture any errors along with the query and data for traceability
            except ServiceUnavailable as exception:
                logging.error("{query} raised an error: \n {exception}".format(
                    query=query, exception=exception))
                raise

    #Vishnu: 1 Jun 2022: created this f() to list all nodes with usecase label
    def find_all_usecase_actors(self, usecase_id):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_all_usecase_actors, usecase_id)
            i=0
            for row in result:
                i=i+1
                print("Found actor-"+ str(i) +": {row}".format(row=row))

    @staticmethod
    def _find_and_return_all_usecase_actors(tx, usecase_id):
        query = (
            "MATCH (n) "
            "WHERE (n.usecase = '"+usecase_id+"') "
            "RETURN n.name AS name"
        )
        result = tx.run(query)
        return [row["name"] for row in result]

    #Vishnu: 21 Jun 2022: created this f() to cleanup the DB
    #CAUTION: will delete everything
    def cleanup_db(self):
        with self.driver.session() as session:
            result = session.read_transaction(
                self._count_nodes_in_db)
            for row in result:
                print("Found {row} actors, deleting! ".format(row=row['count']))
            result = session.write_transaction(
                self._cleanup_db)

    @staticmethod
    def _count_nodes_in_db(tx):
        query = (
            "MATCH (n) "
            "return count(n) as count"
        )
        result = tx.run(query)
        try:
            return [{"count": row["count"]} 
                    for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    #Vishnu: 22 June 2022: created this f() 
    #(over)writes a prop to a node
    def writePropToNode(self, actor_name, propertyName, propertyVal):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._write_property_and_return_existing_node, 
                    actor_name, 
                    propertyName, 
                    propertyVal)
        
            # we expect only 1 node. Do we need the for loop below?
            for row in result:
                print("added prop "+
                        propertyName+
                        " = "+
                        propertyVal+
                        " to node: {n1}".format(n1=row['n1']))

    #Vishnu: 22 June 2022: created this f() 
    #(over)writes a prop to a rel
    def writePropToRel(self, a1, rel_name, a2, propertyName, propertyVal):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._write_property_and_return_existing_rel, 
                    a1, rel_name, a2,
                    propertyName, 
                    propertyVal)
        
            # we expect only 1 node. Do we need the for loop below?
            for row in result:
                print("added prop "+
                        propertyName+
                        " = "+
                        propertyVal+
                        " to rel: {r1}".format(r1=row['r1']))

    @staticmethod
    def _cleanup_db(tx):
        query = (
            "MATCH (n) "
            "detach delete n"
        )
        result = tx.run(query)
        try:
            return 
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise
    
    #Vishnu: 29 Aug 2022
    #https://github.com/vrra/FGAN-Build-a-thon-2022/issues/52
    def del_node_in_db(self, node_name):
        with self.driver.session() as session:
            node_already_exists = session.read_transaction(
                self._find_and_return_existing_node_label, node_name)
            if not node_already_exists:
                print(f"\"{node_name}\" does not exist! ")
            result = session.write_transaction(
                self._del_node_in_db, node_name)
            for row in result:
                    print("Deleted node: {n1}".format(n1=row['name']))
            
    #Vishnu: 29 Aug 2022
    #https://github.com/vrra/FGAN-Build-a-thon-2022/issues/52
    @staticmethod
    def _del_node_in_db(tx, actor1_name):
        query = (
            "MATCH (n) "
            "WHERE (n.name = '"+actor1_name+"') "
            "with n, n.name as name "
            "detach delete n "
            "return name"
        )
        result = tx.run(query)
        try:
            return [{"name": row["name"]} 
                    for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise


neo4j_uri= os.environ.get("NEO4J_URI")
neo4j_user= os.environ.get("NEO4J_USER")
neo4j_pass= os.environ.get("NEO4J_PASSWD")
my_issue_label=os.environ.get("MY_ISSUE_LABEL")


print("neo4j_uri = "+neo4j_uri)
print("neo4j_user = "+neo4j_user)

app = App(neo4j_uri, neo4j_user, neo4j_pass)


issue_json= os.environ.get("SCRIPTS_DIR") + '/issue.out'
print("current working dir = "+os.getcwd())
print("issue_json = "+issue_json)

with open(issue_json, 'r') as json_file:
    json_object = json.load(json_file)

issue_body=json_object["event"]["issue"]["body"]
issue_body_list=issue_body.split("###")
#print("issue_body_list= ", issue_body_list)

issue_label=json_object["event"]["issue"]["labels"][0]["name"]
print("issue_label= ", issue_label)

if (my_issue_label == issue_label):
    print("This is a survey submission! lets process it!")

    #NOTE- we use max split as 1 to avoid false positive of double \n\n in the body.
    bank_visit_count=issue_body_list[1].split("\n\n", 1)
    bank_visit_count=bank_visit_count[1]

    print("bank_visit_count= ", bank_visit_count)
    print("bank_visit_count = ", bank_visit_count[0])

else:
    print("This is not a survey submission! lets forget it!")
