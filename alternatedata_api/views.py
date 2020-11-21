from django.shortcuts import render
from django.http import JsonResponse, Http404
from django.views.decorators.csrf import csrf_exempt
from alternatedata_api.reports_module.calls_log_analysis import calls_log_report
from alternatedata_api.reports_module.contact_tagging import phonebook_mapping
from alternatedata_api.reports_module.data_from_message import message_analysis
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
import pandas as pd
# from rest_framework.decorators import api_view, permission_classes
# from .permissions import Check_API_KEY_Auth
# Create your views here.

# @api_view(['GET'])
# @permission_classes((Check_API_KEY_Auth, ))
# @csrf_exempt
class call_logs_api(APIView):
    permission_classes = (IsAuthenticated,)
    def get(self, request):
         file_path='alternatedata_api/reports_module/dump_alternate_csv/call_logs_temp.csv'
         obj=calls_log_report()
         data=obj.json_reports(file_path)
         return JsonResponse(data)


# def call_logs_api(request):
#  if request.method == 'GET':
#
#      # user_id=request.GET['user_id']
#
#      file_path='alternatedata_api/reports_module/dump_alternate_csv/call_logs_temp.csv'
#      obj=calls_log_report()
#      data=obj.json_reports(file_path)
#
#      return JsonResponse(data)

# @csrf_exempt
class missedcallreport_api(APIView):
    permission_classes = (IsAuthenticated,)
    def get(self,request):
        file_path='alternatedata_api/reports_module/dump_alternate_csv/call_logs_temp.csv'
        obj=calls_log_report()
        data=obj.missedcallreports(file_path)
        return JsonResponse(data)

# @csrf_exempt
class contact_tagging_api(APIView):
    permission_classes = (IsAuthenticated,)
    def get(self,request):
        custom_entity_input=eval(request.GET['custom_entity_input'])
        file_path='alternatedata_api/reports_module/dump_alternate_csv/phone_book.csv'
        # custom_entity_input={'office':['maam','mam','sir']}
        obj=phonebook_mapping()
        data=obj.getjsonoutput(file_path,custom_entity_input)

        return JsonResponse(data)

# @csrf_exempt
class bank_message_api(APIView):
    # if request.method ==  'GET':
    permission_classes = (IsAuthenticated,)
    def get(self,request):
        file_path='alternatedata_api/reports_module/dump_alternate_csv/mail_and_messages.csv'
        obj=message_analysis()
        dataframe=pd.read_csv(file_path)
        data=obj.predict_labels(dataframe)
        json_data=obj.newformat_function()

        return JsonResponse(json_data)

# @csrf_exempt
class classify_message_api(APIView):
    permission_classes = (IsAuthenticated,)
    def get(self,request):
        file_path='alternatedata_api/reports_module/dump_alternate_csv/mail_and_messages.csv'
        obj=message_analysis()
        dataframe=pd.read_csv(file_path)
        data=obj.predict_labels(dataframe)
        json_data=obj.classify_message()

        return JsonResponse(json_data)

# @csrf_exempt
class avg_monthly_bal_api(APIView):
    # if request.method ==  'GET':
    permission_classes = (IsAuthenticated,)
    def get(self,request):
        file_path='alternatedata_api/reports_module/dump_alternate_csv/mail_and_messages.csv'
        obj=message_analysis()
        dataframe=pd.read_csv(file_path)
        data=obj.predict_labels(dataframe)
        obj.bank_data2json(data)
        json_data=obj.avg_monthly_bal()

        return JsonResponse(json_data)

# from rest_framework.response import Response
#
# from rest_framework.authtoken.models import Token
# from rest_framework.exceptions import ParseError
# from rest_framework import status
#
# from django.contrib.auth.models import User
# from rest_framework.views import APIView



# def verify_token(request):
#     print(request.auth)
#     return JsonResponse({'status':"True"})


# # Create your views here.
# @csrf_exempt
# class TestView(APIView):
#     """
#     """
#
#     def get(self,request):
#         return Response({'detail': "GET Response"})
#
#     def post(self, request):
#         try:
#             data = request.DATA
#         except ParseError as error:
#             return Response(
#                 'Invalid JSON - {0}'.format(error.detail),
#                 status=status.HTTP_400_BAD_REQUEST
#             )
#         if "user" not in data or "password" not in data:
#             return Response(
#                 'Wrong credentials',
#                 status=status.HTTP_401_UNAUTHORIZED
#             )
#
#         user = User.objects.first()
#         if not user:
#             return Response(
#                 'No default user, please create one',
#                 status=status.HTTP_404_NOT_FOUND
#             )
#
#         token = Token.objects.get_or_create(user=user)
#
#         return Response({'detail': 'POST answer', 'token': token[0].key})



# class TestView(APIView):
#     permission_classes = (IsAuthenticated,)
#     def get(self, request):
#         content = {'message': 'Hello, World!'}
#         return Response(content)
