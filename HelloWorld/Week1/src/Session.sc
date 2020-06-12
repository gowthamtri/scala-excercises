1 + 2

def abs(x: Double): Double = if (x < 0) -x else x
abs(-10)
abs(20)
abs(-203)

def isGoodEnough(guess: Double, x: Double) = abs((guess * guess) - x) / x < 1e-3

def improveGuess(guess: Double, x: Double) = (guess + x / guess) / 2

def sqrt(x: Double, guess: Double): Double =
  if (isGoodEnough(guess, x)) guess
  else sqrt(x, improveGuess(guess, x))

sqrt(25, 2)
sqrt(1e-6, 1)
