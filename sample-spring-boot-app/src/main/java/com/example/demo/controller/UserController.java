package com.example.demo.controller;

import com.example.demo.model.User;
import com.example.demo.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/users")
public class UserController {

    @Autowired
    private UserService userService;

    @GetMapping
    public ResponseEntity<List<User>> getAllUsers() {
        List<User> users = userService.getAllUsers();
        return ResponseEntity.ok(users);
    }

    @GetMapping("/{id}")
    public ResponseEntity<User> getUserById(@PathVariable Long id) {
        return userService.getUserById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/email/{email}")
    public ResponseEntity<User> getUserByEmail(@PathVariable String email) {
        return userService.getUserByEmail(email)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping
    public ResponseEntity<User> createUser(@RequestBody User user) {
        User createdUser = userService.createUser(user);
        return ResponseEntity.status(HttpStatus.CREATED).body(createdUser);
    }

    @PutMapping("/{id}")
    public ResponseEntity<User> updateUser(@PathVariable Long id, @RequestBody User user) {
        try {
            User updatedUser = userService.updateUser(id, user);
            return ResponseEntity.ok(updatedUser);
        } catch (RuntimeException e) {
            return ResponseEntity.notFound().build();
        }
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteUser(@PathVariable Long id) {
        userService.deleteUser(id);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/count")
    public ResponseEntity<Long> countUsers() {
        long count = userService.countUsers();
        return ResponseEntity.ok(count);
    }

    @GetMapping("/slow-query")
    public ResponseEntity<List<User>> executeLongRunningQuery() {
        List<User> users = userService.executeLongRunningQuery();
        return ResponseEntity.ok(users);
    }

    @GetMapping("/slow-sales-query")
    public ResponseEntity<List<Map<String, Object>>> executeSlowSalesQuery() {
        List<Map<String, Object>> results = userService.executeSlowSalesQuery();
        return ResponseEntity.ok(results);
    }

    @GetMapping("/active-sales-query")
    public ResponseEntity<List<Map<String, Object>>> executeActiveSalesQuery() {
        List<Map<String, Object>> results = userService.executeActiveSalesQuery();
        return ResponseEntity.ok(results);
    }

    @GetMapping("/slow-product-query")
    public ResponseEntity<List<Map<String, Object>>> executeSlowProductQuery() {
        List<Map<String, Object>> results = userService.executeSlowProductQuery();
        return ResponseEntity.ok(results);
    }

    // NEW ENDPOINTS for Active Query Testing

    @GetMapping("/active-aggregation-query")
    public ResponseEntity<List<Map<String, Object>>> executeLongAggregationQuery() {
        List<Map<String, Object>> results = userService.executeLongAggregationQuery();
        return ResponseEntity.ok(results);
    }

    @GetMapping("/active-blocking-query")
    public ResponseEntity<List<Map<String, Object>>> executeBlockingQuery() {
        List<Map<String, Object>> results = userService.executeBlockingQuery();
        return ResponseEntity.ok(results);
    }

    @GetMapping("/active-cpu-query")
    public ResponseEntity<List<Map<String, Object>>> executeCpuIntensiveQuery() {
        List<Map<String, Object>> results = userService.executeCpuIntensiveQuery();
        return ResponseEntity.ok(results);
    }

    @GetMapping("/active-io-query")
    public ResponseEntity<List<Map<String, Object>>> executeIoIntensiveQuery() {
        List<Map<String, Object>> results = userService.executeIoIntensiveQuery();
        return ResponseEntity.ok(results);
    }

    // SINGLE FOCUSED ENDPOINT: Person UPDLOCK with blocking
    @GetMapping("/person-updlock-query")
    public ResponseEntity<List<Map<String, Object>>> executePersonUpdlockQuery() {
        List<Map<String, Object>> results = userService.executePersonUpdlockQuery();
        return ResponseEntity.ok(results);
    }
}
