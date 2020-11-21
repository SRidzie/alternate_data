from django.urls import path
from . import views
app_name = 'alternatedata_api'
urlpatterns = [
    path('calls_log_report/',views.call_logs_api.as_view()),
    path('contact_tagging/',views.contact_tagging_api.as_view()),
    path('bankmessage_report/',views.bank_message_api.as_view()),
    path('classify_message/',views.classify_message_api.as_view()),
    path('avg_monthly_bal/',views.avg_monthly_bal_api.as_view()),
    path('missedcallreports/',views.missedcallreport_api.as_view()),
    # path("testtoken/",views.TestView.as_view()),
    # path("vt/",views.verify_token),
]
