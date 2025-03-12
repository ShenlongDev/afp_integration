from decimal import Decimal
from django.db.models import Sum, Case, When, Value, F, Q, DecimalField
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from integrations.models.toast.raw import ToastOrder


class SalesSummaryAPIView(APIView):
    """
    GET API endpoint that accepts startdate, enddate, and option.
    Example URL: /sales-summary/?startdate=20250301&enddate=20250303&option=Netsales
    """
    def get(self, request, format=None):
        # Get parameters from URL query parameters.
        start_date = request.query_params.get('startdate')
        end_date = request.query_params.get('enddate')
        option = request.query_params.get('option')

        # Validate presence of parameters.
        if not start_date or not end_date or not option:
            return Response(
                {"error": "startdate, enddate, and option parameters are required."},
                status=status.HTTP_400_BAD_REQUEST
            )

        option = option.lower()
        if option not in ["netsales", "totalamounts"]:
            return Response(
                {"error": "option must be either 'Netsales' or 'TotalAmounts'."},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            start_date_int = int(start_date)
            end_date_int = int(end_date)
        except ValueError:
            return Response(
                {"error": "startdate and enddate must be numeric in the format YYYYMMDD."},
                status=status.HTTP_400_BAD_REQUEST
            )
            
        # Validate that the start date is not after the end date.
        if start_date_int > end_date_int:
            return Response(
                {"error": "startdate should be less than or equal to enddate."},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # (Optional) Validate that the dates are exactly in YYYYMMDD format (8 digits)
        if len(start_date) != 8 or len(end_date) != 8:
            return Response(
                {"error": "startdate and enddate must be in the format YYYYMMDD."},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Determine which field to aggregate.
        field_name = "order_net_sales" if option == "netsales" else "toast_sales"

        # Build a queryset that considers orders with either a business_date or a refund_business_date in the range.
        orders = ToastOrder.objects.filter(
            Q(business_date__gte=start_date_int, business_date__lte=end_date_int) |
            Q(refund_business_date__gte=str(start_date_int), refund_business_date__lte=str(end_date_int))
        )

        # Aggregate primary amount and refund adjustment.
        result = orders.aggregate(
            total_primary=Sum(
                Case(
                    When(business_date__gte=start_date_int, business_date__lte=end_date_int, then=F(field_name)),
                    default=Value(Decimal("0.00")),
                    output_field=DecimalField()
                )
            ),
            total_refund=Sum(
                Case(
                    # Only subtract refunds if the refund_business_date is in range and
                    # the order's original business_date is different (i.e. the refund belongs to another day)
                    When(
                        Q(refund_business_date__gte=str(start_date_int)) &
                        Q(refund_business_date__lte=str(end_date_int)) &
                        ~Q(business_date=F('refund_business_date')),
                        then=F('total_refunds')
                    ),
                    default=Value(Decimal("0.00")),
                    output_field=DecimalField()
                )
            )
        )

        total_primary = result.get("total_primary") or Decimal("0.00")
        total_refund = result.get("total_refund") or Decimal("0.00")
        total_value = total_primary - total_refund

        # Optional: ensure that the total does not go negative.
        if total_value < 0:
            total_value = Decimal("0.00")

        data = {
            "startdate": start_date,
            "enddate": end_date,
            "option": option,
            "total": str(total_value)
        }
        return Response(data, status=status.HTTP_200_OK)
