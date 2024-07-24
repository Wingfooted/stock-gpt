import jax
import jax.random as random
import jax.numpy as jnp

import flax
from flax.linen import module as nn
from typing import Sequence


class Model(nn.Module):
    features: Sequence[int]

    @nn.compact
    def __call__(self, inputs):
        x = inputs
        for feat in self.features[:-1]:
            x = nn.Dense(feat)(x)
            x = nn.relu(x)
        x = nn.Dense(self.features[-1])(x)
        return x
