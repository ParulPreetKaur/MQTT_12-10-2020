def get_loc():
  Lane = 5
  print("Hello from a function")
  if Lane <= 10:
    print("Lane is:" + str(Lane) )
    Direction = "Travelling from East to West"
    return(Direction)

output = get_loc()
print(output)




