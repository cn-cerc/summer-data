package cn.cerc.db.core;

import java.security.SecureRandom;

public class UserAccountGenerator {
    private static final char[] CHARACTERS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
    private static final int MAX_RANDOM_VALUE = 36 * 36 * 36 * 36;

    public static String generateUserAccount() {
        SecureRandom random = new SecureRandom();
        int randomNumber = random.nextInt(MAX_RANDOM_VALUE);
        String suffix = convertTo36Radix(randomNumber);
        String paddedSuffix = String.format("%4s", suffix).replace(' ', '0');
        return "220701" + paddedSuffix;
    }

    private static String convertTo36Radix(int number) {
        StringBuilder sb = new StringBuilder();
        do {
            int digit = number % 36;
            sb.append(CHARACTERS[digit]);
            number /= 36;
        } while (number > 0);
        return sb.reverse().toString();
    }

    public static void main(String[] args) {
        long startTime = System.nanoTime();
        String userAccount = UserAccountGenerator.generateUserAccount();
        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        System.out.println("Generated user account " + userAccount + " in " + duration + " nanoseconds.");
    }
}
