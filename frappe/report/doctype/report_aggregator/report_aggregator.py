# -*- coding: utf-8 -*-
# Copyright (c) 2018, Frappe Technologies and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
from frappe.model.document import Document
from frappe.desk.query_report import run as execute_report
import re
from six import string_types
from frappe.utils import nowdate, cstr


class ReportAggregator(Document):
	def validate(self):
		if not self.report:
			frappe.throw('Report name is mandatory')

	def before_insert(self):
		# pass
		if not self.doc_type:
			self.doc_type = remove_special_char(self.report).replace('_', ' ').title() + " Aggregator"

	# columns : list of columns from report
	def create_or_update_doctype(self, columns):

		#set because it takes only unique name and provides difference functionality
		# new_field_names = set([1,2,3,4,5,1,1,4])
		# new_field_names = {1,2,3,4,5}
		# doctype_fields = {1,2,3}
		# final_list = new_field_names - doctype_fields
		# output: final_list = {4,5}

		new_field_names = set(["_".join([col.fieldname, col.fieldtype]).lower() for col in columns])

		doc = None
		doc_name = frappe.db.get_value('DocType', self.doc_type)

		if self.doc_type and doc_name:
			doc = frappe.get_doc('DocType', doc_name)
			fields = doc.fields
			new_field_names = new_field_names - set([field.fieldname for field in fields])

		if not new_field_names:
			return

		if not doc and not doc_name:
			doc = frappe.get_doc({
				'doctype': 'DocType',
				'__newname': self.doc_type,
				'name': self.doc_type,
				'module': 'Report',
				'autoname': "hash",
				'custom': 1,
				'track_changes': 0,
				'in_create': 1,
			})

			doc.append('permissions', {
				'role': 'System Manager', 'read': 1,
				'write': 1, 'create': 1, 'report': 1, 'export': 1
			})

			doc.append('fields', {
				'label': "Aggregation Date",
				'in_list_view': 1,
				'fieldtype': 'Date',
				'fieldname': '__date',
				'read_only': 1,
				'search_index': 1
			})
			doc.insert()


		for field in new_field_names:
			# field will have fieldname_fieldtype,
			# so split and take the last one for fields and rest is for label name
			_f = field.split('_')
			fieldtype = _f.pop()

			if fieldtype.title() not in frappe.get_meta('DocField').get_field('fieldtype').options.split('\n'):
				_f.append(fieldtype)
				fieldtype = 'Data'

			_f = " ".join(_f)
			doc.append('fields', {
				'label': _f.title(),
				'fieldtype': fieldtype.title()  or 'Data',
				'fieldname': field,
				'read_only': 1,
				'in_list_view': 1,
				})

		doc.flags.ignore_permissions = 1
		doc.save()


@frappe.whitelist()
def take_aggregate(docname):
	frappe.utils.background_jobs.enqueue(execute, queue="long", docname=docname);

def execute(docname):
	# this will create a in memory object
	doc = frappe.get_doc('Report Aggregator', docname)
	if doc.disabled:
		frappe.msgprint("Report Aggregator is disabled")
		return

	filters = frappe._dict()

	for row in doc.filters:
		if row.field_value.startswith('{{'):
			row.field_value = frappe.render_template(row.field_value, {'frappe': frappe})

		filters.setdefault (row.field_name, row.field_value)

	report_res = frappe._dict(execute_report(doc.report, filters))

	if report_res.result and report_res.columns:
		report_res.columns = [ parse_column(column) for column in report_res.columns]
		doc.create_or_update_doctype(report_res.columns)

		for row in report_res.result:
			data_dict = frappe._dict({'doctype': doc.doc_type, '__date': nowdate()})
			for idx, col in enumerate(report_res.columns):
				data_dict.setdefault("_".join([col.fieldname, col.fieldtype]).lower(), row[idx])

			frappe.get_doc(data_dict).insert()

def parse_column(column):

	# in tree type report the column is in dict format
	if isinstance(column, string_types):
		c_details = column.split(':')
		fieldname = remove_special_char(c_details[0])
		fieldtype = c_details[1] if len(c_details) > 1 else 'Data'

		return frappe._dict({'fieldname': fieldname, 'fieldtype': fieldtype})

	return column

def remove_special_char(name):

	# columns in report are generally in the form column_name:Datatype:width
	# so "column_name:Datatype:width".split(':')[0] will return column_name
	name = name.split(':')[0]
	name = name.replace(' ', '_').strip().lower()

	# re.sub replace all the match with ''
	name = re.sub("[\W]", '', name, re.UNICODE)
	return name


def run_hourly():
	now = frappe.utils.now_datetime()
	hour = now.hour

	for row in frappe.get_all('Report Aggregator', {'at_hour' : hour, 'disabled': 0}):
		take_aggregate(row.name)
