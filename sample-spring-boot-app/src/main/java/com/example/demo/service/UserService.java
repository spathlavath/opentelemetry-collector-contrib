package com.example.demo.service;

import com.example.demo.model.User;
import com.example.demo.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class UserService {

    @Autowired
    private UserRepository userRepository;

    public List<User> getAllUsers() {
        return userRepository.findAll();
    }

    public Optional<User> getUserById(Long id) {
        return userRepository.findById(id);
    }

    public Optional<User> getUserByEmail(String email) {
        return userRepository.findByEmail(email);
    }

    public User createUser(User user) {
        return userRepository.save(user);
    }

    public User updateUser(Long id, User userDetails) {
        User user = userRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("User not found with id: " + id));

        user.setName(userDetails.getName());
        user.setEmail(userDetails.getEmail());
        user.setRole(userDetails.getRole());

        return userRepository.save(user);
    }

    public void deleteUser(Long id) {
        userRepository.deleteById(id);
    }

    public long countUsers() {
        return userRepository.count();
    }

    public List<User> executeLongRunningQuery() {
        return userRepository.executeLongRunningQuery();
    }

    public List<Map<String, Object>> executeSlowSalesQuery() {
        return userRepository.executeSlowSalesQuery();
    }

    public List<Map<String, Object>> executeActiveSalesQuery() {
        return userRepository.executeActiveSalesQuery();
    }

    public List<Map<String, Object>> executeSlowProductQuery() {
        return userRepository.executeSlowProductQuery();
    }

    // NEW: Active query methods
    public List<Map<String, Object>> executeLongAggregationQuery() {
        return userRepository.executeLongAggregationQuery();
    }

    public List<Map<String, Object>> executeBlockingQuery() {
        return userRepository.executeBlockingQuery();
    }

    public List<Map<String, Object>> executeCpuIntensiveQuery() {
        return userRepository.executeCpuIntensiveQuery();
    }

    public List<Map<String, Object>> executeIoIntensiveQuery() {
        return userRepository.executeIoIntensiveQuery();
    }

    // SINGLE FOCUSED QUERY: Person UPDLOCK
    public List<Map<String, Object>> executePersonUpdlockQuery() {
        return userRepository.executePersonUpdlockQuery();
    }
}
