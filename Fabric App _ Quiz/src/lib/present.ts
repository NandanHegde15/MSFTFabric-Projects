import type { Question } from '@/data/questions';

import { shuffle } from './shuffle';

/**
 * Randomise the order of a question's choices, remapping the answer indexes.
 *
 * The bank is authored with the correct choice written first, which is fine for
 * reading and reviewing the source but would make the rendered quiz trivially
 * gameable. Every deck is dealt through this, so position carries no signal.
 */
export function presentQuestion(question: Question): Question {
  const order = shuffle(question.choices.map((_, i) => i));

  return {
    ...question,
    choices: order.map((i) => question.choices[i]),
    answer: question.answer
      .map((a) => order.indexOf(a))
      .sort((a, b) => a - b),
  };
}

export function presentAll(questions: Question[]): Question[] {
  return questions.map(presentQuestion);
}
