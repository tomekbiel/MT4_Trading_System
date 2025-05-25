import numpy as np
import matplotlib.pyplot as plt


def tree_mandelbrot(width=800, height=800, max_iter=100, k=3):
    # Generowanie siatki punktów (c)
    x = np.linspace(-2.5, 1.5, width)
    y = np.linspace(-2, 2, height)
    c = x[:, np.newaxis] + 1j * y[np.newaxis, :]

    # Inicjalizacja
    z = np.zeros_like(c, dtype=np.complex128)
    fractal = np.zeros((width, height))

    for i in range(max_iter):
        mask = np.abs(z) < 4  # Tylko punkty, które nie uciekły
        z[mask] = z[mask] ** k + np.sin(np.imag(c[mask])) * c[mask] + c[mask]
        fractal += mask  # Zlicz iteracje

    return fractal.T


# Generowanie i rysowanie
plt.figure(figsize=(10, 10))
fractal = tree_mandelbrot(k=3, max_iter=50)
plt.imshow(fractal, cmap='viridis', extent=(-2.5, 1.5, -2, 2))
plt.axis('off')
plt.savefig('tree_mandelbrot.png', dpi=300, bbox_inches='tight')
plt.show()