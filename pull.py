%dw 1.0
%output application/java
---
payload map {
	AccountNumber : $."account_number",
	AccountSource : $."account_source",
	CreatedDate : $."creation_date",
	Id : $."id",
	Name : $."account_name"
}