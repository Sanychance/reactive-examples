package guru.springframework.reactiveexamples;

import guru.springframework.reactiveexamples.domain.Person;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

class PersonRepositoryImplTest {

    PersonRepositoryImpl personRepository;
    @BeforeEach
    void setUp() {
        personRepository = new PersonRepositoryImpl();
    }

    @Test
    void getByIdBlock() {
        Mono<Person> personMono = personRepository.getById(2);
        Person person = personMono.block();
        System.out.println(person.toString());
    }

    @Test
    void getByIdSubscribe() {
        Mono<Person> personMono = personRepository.getById(1);
        StepVerifier.create(personMono).expectNextCount(1).verifyComplete(); //number of interaction ==1
        personMono.subscribe(person -> System.out.println(person.toString()));
    }

    @Test
    void getByIdSubscribeNotFound() {
        Mono<Person> personMono = personRepository.getById(3);
        StepVerifier.create(personMono).expectNextCount(0).verifyComplete(); //number of interaction ==1
        personMono.subscribe(person -> System.out.println(person.toString()));
    }

    @Test
    void getByIdMapFunction() {
        Mono<Person> personMono = personRepository.getById(1);
        personMono
                .map(person -> {
                    System.out.println(person.toString()); //nothing will happen as there is no backpressure
                        return person.getFirstName();
                }).subscribe(firstName -> System.out.println("Fname: "+firstName));
    }


    @Test
    void fluxTestBlockFirst() {
        Flux<Person> personFlux = personRepository.findAll();
        Person person = personFlux.blockFirst();
        System.out.printf(person.toString());
    }

    @Test
    void testFluxSubscribe() {
        Flux<Person> personFlux = personRepository.findAll();

        StepVerifier.create(personFlux).expectNextCount(2).verifyComplete();
        personFlux.subscribe(person -> {
            System.out.println(person.toString());
        });
    }


    @Test
    void testFluxToListMono() {
        Flux<Person> personFlux = personRepository.findAll();

        Mono<List<Person>> personListMono = personFlux.collectList();
        personListMono.subscribe(personList -> {
            personList.forEach(person -> {
                System.out.println(person.toString());
            });
        });
    }

    @Test
    void testFluxFilter_findPersonById() {
        Flux<Person> personFlux = personRepository.findAll();

        Mono<Person> personMono = personFlux.filter(person -> person.getId() == 1).next();
        personMono.subscribe(person -> System.out.println(person.toString()));
    }

    @Test
    void testFluxFindPersonByIdNotFound() {
        Flux<Person> personFlux = personRepository.findAll();

        Mono<Person> personMono = personFlux.filter(person -> person.getId() == 3).next(); //return silenty onError
        personMono.subscribe(person -> System.out.println(person.toString()));
    }


    @Test
    void testFluxFindPersonByIdNotFoundWithException() {
        Flux<Person> personFlux = personRepository.findAll();

        final Integer id = 3;
        Mono<Person> personMono = personFlux.filter(person -> person.getId() == id).single();  //return on error with exception or anything that we would like tod do
        personMono.doOnError(ex -> {
            System.out.println(ex.getStackTrace());
        }).onErrorReturn(Person.builder().id(id).build())
                .subscribe(person ->
                        System.out.println(person.toString()));
    }

}