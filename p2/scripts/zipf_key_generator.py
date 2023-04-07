import random
import math

def zipf(n, theta):
    alpha = 1.0 / (1 - theta)

    # Calculate the normalization constant (zetan)
    zetan = zeta(n, theta)

    # Calculate the denominator of the zipf equation (eta)
    eta = (1 - pow(2 / n, 1 - theta)) / (1 - zeta(2, theta) / zetan)

    # Generate a random value between 0 and 1   
    u = random.random()

    # Calculate the corresponding item rank for the random value using the zipf equation
    uz = u * zetan
    if uz < 1:
        return 1
    if uz < 1 + pow(0.5, theta):
        return 2
    return 1 + int(n * pow(eta * u - eta + 1, alpha))

def zeta(n, theta):
    # Calculate the zeta function for a range of integers from 1 to n
    zetan = 0
    for i in range(1, n + 1):
        zetan += 1 / pow(i, theta)

    return zetan

# Example usage
N = 1000  # Number of items
theta = 0.9  # Zipfian parameter

# Generate a Zipfian distribution
frequencies = [0] * N
for i in range(100000):
    rank = zipf(N, theta)
    frequencies[rank - 1] += 1

# Normalize the frequencies
# print(frequencies)
total = sum(frequencies)
frequencies = [freq / total for freq in frequencies]

# Generate a workload of 100 requests
requests = []
for i in range(100):
    rank = random.choices(range(N), weights=frequencies)[0]
    requests.append(rank)

# Print the first 10 requests
# print(requests)
# print(len(requests))
filename='zipfian_keys.csv'
with open(filename, 'w') as file:
    file.write(','.join(str(key) for key in requests))