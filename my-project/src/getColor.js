export function getColor(value, maxValue) {
  // Clamp value between 0 and maxValue
  value = Math.max(0, Math.min(value, maxValue));

  // Calculate the ratio of value to maxValue
  const ratio = value / maxValue;

  // Fast transition from green to yellow (use an exponential curve)
  let curveRatio = ratio;
  if (ratio < 0.5) {
    curveRatio = Math.pow(ratio, 3); // Fast transition from green to yellow using cubic curve
  } else {
    // Gradual transition from yellow to red (logarithmic curve)
    curveRatio =
      0.5 + Math.log(1 + (ratio - 0.5) * (Math.E - 1)) / Math.log(Math.E); // Logarithmic for the upper half
  }

  // Interpolate between green and red based on the non-linear curve
  const red = Math.round(255 * curveRatio); // Red increases as the value gets closer to maxValue
  const green = Math.round(255 * (1 - curveRatio)); // Green decreases as the value gets closer to maxValue

  return `rgb(${red}, ${green}, 0)`; // Blue remains 0
}
