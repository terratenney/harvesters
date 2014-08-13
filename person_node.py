from py2neo import neo4j, node, rel

class Person(object):
    def __init__(self):
        
        self.first_name= None
        self.last_name= None
        self.dob= None
        self.age=None
        self.sex=None

        self.high_school= None
        self.college= None
        self.job= None

        self.current_city= None

        self.from_city= None
        self.citizenship= None
        self.primary_activity_area= None

        self.twitter_account= None
        self.facebook_account= None
        self.google_account= None
        self.flikr_account= None
        
        self.relationship_types_available= None
        self.relationships=None
        self.topics= None
        
    def get_node(self):
        node = {"first_name":self.first_name,"last_name": self.last_name,"dob":self.dob,"age":self.age,"sex":self.gender,"high_school": self.high_school,"college": self.college,"job": self.job,"current_city":self.current_city,"from_city":self.from_city,"primary_activity_area":self.primary_activity_area, "twitter_account":self.twitter_account,"facebook_account":self.facebook_account,"google_account": self.google_account,"flikr_account": self.flikr_account,"relationship_types": self.relationship_types_available,"relationships": self.relationships,"topics":self.topics}
        return node
