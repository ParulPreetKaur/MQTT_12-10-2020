from datetime import datetime
now = datetime.now()
t=str(now.time())
c=t.split(':')
print(c[2])
print(c)
c[2]='00'
print(c[2])
string=c[0]+":"+c[1]+":"+c[2]
print(string)
d=now.date()
print(type(d))
print(str(d))
created_at=str(d) + " " +string
print(created_at)
