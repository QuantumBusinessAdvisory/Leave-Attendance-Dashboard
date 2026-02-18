# config.py

# API Authentication
# The user specified format: "Authorization": "token <api_key>:<api_secret>"
API_HEADERS = {
    "Authorization": "token 762913b0eb9f140:1205f410c1b7b31",
    "Content-Type": "application/json"
}

# List of API Endpoints
# We will add more here later.
API_ENDPOINTS = {
    "leave_balance": "https://hr.qbadvisory.com/api/method/hrms.api.employee.get_all_employees_leave_balance",
    "leave_applications": "https://hr.qbadvisory.com/api/method/hrms.api.employee.get_all_employees_leave_applications",
    "attendance": "https://hr.qbadvisory.com/api/method/hrms.api.employee.get_all_attendance",
    "timesheet": "https://hr.qbadvisory.com/api/method/hrms.api.employee.get_all_users_timesheet_details",
    "project_allocations": "https://hr.qbadvisory.com/api/method/hrms.api.employee.get_user_project_allocations",
    "projects_details": "https://hr.qbadvisory.com/api/method/hrms.api.employee.get_all_projects_details",
    "managers": "https://hr.qbadvisory.com/api/method/hrms.api.employee.get_all_managers_with_departments",
    "holidays": "https://hr.qbadvisory.com/api/method/hrms.api.employee.get_all_holidays",
    "users_details": "https://hr.qbadvisory.com/api/method/hrms.api.employee.get_all_users_details"
}
