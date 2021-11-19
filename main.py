from flask import Flask
from flask_restful import Api, Resource, reqparse, abort, fields, marshal_with
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
import numpy as np
import re
from os import listdir
from os.path import isfile, join

app = Flask(__name__)
api = Api(app)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///AdditionalData.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

class ProteinModel(db.Model):
	# the primary protein database where both name and id are primary keys
	__tablename__ = "ProteinDB"
	__table_args__ = {'extend_existing':True}
	id = db.Column(db.String(100), primary_key=True)	# primary_key means unique identifier
	name = db.Column(db.String(100), nullable=False)

class ProteinSource(db.Model):
	# association table to model many-to-many relationships between tables
	__tablename__ = 'protein_source'
	__table_args__ = {'extend_existing':True}
	relation_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
	protein_id = db.Column("id", db.String(100), db.ForeignKey('ProteinDB.id'), primary_key=False)
	protein_name = db.Column("name", db.String(100), db.ForeignKey('ProteinDB.name'), primary_key=False)
	dataset_id = db.Column(db.String(100))
	
db.drop_all()
db.create_all()	# re-run it will erase our data

# mypath = "/Users/jh2313/JupyterProjects/Front-end web development/database/AdditionalDatasets/"
mypath = "AdditionalDatasets/"
all_files = [f for f in listdir(mypath) if isfile(join(mypath, f))]
if '.DS_Store' in all_files:
	all_files.remove('.DS_Store')

for file in all_files:
	path_to_file = join(mypath,file)
	dataset_id = re.sub('.csv','',file)

	with open(path_to_file,'r') as f:
		data_df = pd.read_csv(f,index_col=0)
		data_df = data_df.rename(columns={"Protein.name1":"name", "ID ":"id"})
		data_df['id'] = np.array(list(map(lambda v: re.sub(r"\(|\)| ","", v),data_df['id'].values)))
		data_df['name'] = np.array(list(map(lambda v: re.sub(r" ","", v),data_df['name'].values)))
		# there are duplicates in dummy datasets
		data_df.drop_duplicates(inplace=True,ignore_index=True)
		con = db.engine
		# check whether database is empty
		# if pd.read_sql("SELECT * FROM protein_source", con).empty:
		# the initial conversion from a CSV dataset to the ProteinDB database
			# data_df.to_sql(name='ProteinDB',con=db.engine,index=False,if_exists='append')
		# else:
		# append new data into existied database, need to check uniqueness
		existed_name = pd.read_sql("SELECT name FROM ProteinDB", con).values.flatten().tolist()
			# only put novel protein into DB
		data_df[~data_df.name.isin(existed_name)].to_sql(name='ProteinDB',con=db.engine,index=False,if_exists='append')

		# record protein source info to protein_source table
		data_df['dataset_id'] = dataset_id
		data_df.to_sql(name='protein_source',con=db.engine,index=False,if_exists='append')

protein_put_args = reqparse.RequestParser()
protein_put_args.add_argument("id", type=str, help="ID of the protein", required=True)
protein_put_args.add_argument("name", type=str, help="name of the protein", required=True)

protein_update_args = reqparse.RequestParser()
protein_update_args.add_argument("id", type=str, help="ID of the protein")
protein_update_args.add_argument("name", type=str, help="name of the protein")

resource_fields = {
	'protein_id': fields.String,
	'protein_name': fields.String,
	'dataset_id': fields.String
}

class Protein(Resource):
	@marshal_with(resource_fields)	# to serialize the output
	def get(self, proteinName):
		result = ProteinSource.query.filter_by(protein_name=proteinName).all()
		if not result:
			abort(404, message="Could not find protein with that name")
		return result

	@marshal_with(resource_fields)	
	def put(self, protein_id):
		args = protein_put_args.parse_args()
		result = ProteinModel.query.filter_by(id=protein_id).first()
		if result:
			abort(409, message="Protein id existed...")

		protein = ProteinModel(id=protein_id, name=args['name'], source=args['source'])
		db.session.add(protein)	# temporarily add to the database
		db.session.commit()		# make it permanent change
		return protein, 201

	@marshal_with(resource_fields)
	def patch(self, protein_id):
		args = protein_update_args.parse_args()
		result = ProteinModel.query.filter_by(id=protein_id).first()
		if not result:
			abort(404, message="Protein doesn't exist, cannot update")
		if args['name']:
			result.name = args['name']
		if args['source']:
			result.source = args['source']
		db.session.commit()
		return result

api.add_resource(Protein, "/protein/<proteinName>")	# "< >" cannot be deleted

if __name__ == "__main__":
	app.run(debug=True)
	# app.run(host='0.0.0.0', port=81)