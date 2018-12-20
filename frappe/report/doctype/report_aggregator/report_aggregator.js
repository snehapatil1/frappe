// Copyright (c) 2018, Frappe Technologies and contributors
// For license information, please see license.txt

frappe.ui.form.on('Report Aggregator', {
	refresh: function(frm) {
			if (!frm.doc.disabled) {
				frm.add_custom_button('Sync', ()=> {
					frappe.call({
						method: 'frappe.report.doctype.report_aggregator.report_aggregator.take_aggregate',
						args: {
							docname: frm.doc.name
						}
					})
				})
				frm.add_custom_button("Show Report", function() {
					frappe.set_route("query-report", frm.doc.report);
				});
			}
		}
});
