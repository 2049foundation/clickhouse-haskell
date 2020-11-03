import matplotlib.pyplot as plt

data = {"Linux": 0.128, "Haskell": 0.196}

client = list(data.keys())
time = list(data.values())
plt.bar(client, time,color ='blue')

plt.xlabel("client") 
plt.ylabel("execution time in second") 
plt.title("Performance comparison") 
plt.show() 